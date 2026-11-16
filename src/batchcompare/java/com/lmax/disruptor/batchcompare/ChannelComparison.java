package com.lmax.disruptor.batchcompare;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.sun.management.OperatingSystemMXBean;
import org.HdrHistogram.Histogram;
import org.springframework.integration.channel.ExecutorChannel;
import org.springframework.integration.channel.PartitionedChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.support.GenericMessage;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Failure;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Row;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Workload;

public final class ChannelComparison
{
    enum Mode
    {
        EXECUTOR, QUEUE, PARTITIONED, DISRUPTOR
    }

    enum Contract
    {
        POINT, KEYED
    }

    record Config(Mode mode, Contract contract, int items, int work, Workload workload, int workers, int capacity, int keys)
    {
        Config
        {
            if (items < 1 || items > 1000000 || work < 0 || work > 100000 || workers < 1 || workers > 16 ||
                    capacity < 1 || capacity > 65536 || Integer.bitCount(capacity) != 1 || keys < 1 || keys > 4096)
            {
                throw new IllegalArgumentException("Invalid item count, work, worker count, capacity or key count");
            }
            if (mode == Mode.PARTITIONED && contract != Contract.KEYED ||
                    contract == Contract.KEYED && mode != Mode.PARTITIONED && mode != Mode.DISRUPTOR)
            {
                throw new IllegalArgumentException("KEYED compares PARTITIONED and DISRUPTOR; POINT compares EXECUTOR, QUEUE and DISRUPTOR");
            }
        }
    }

    record Task(Window window, int id, long startedAt)
    {
    }

    private static final class Stats
    {
        final BitSet seen = new BitSet();
        final long[] nextByKey;
        final Histogram latency = new Histogram(TimeUnit.MINUTES.toNanos(1), 3);
        volatile int completed;

        Stats(final int keys)
        {
            nextByKey = new long[keys];
            for (int i = 0; i < keys; i++)
            {
                nextByKey[i] = i;
            }
        }
    }

    private static final class Local
    {
        Window window;
        Stats stats;
    }

    private static final class Window
    {
        final Config config;
        final long[] expected;
        final int failAt;
        final Thread producer = Thread.currentThread();
        final ConcurrentLinkedQueue<Stats> workers = new ConcurrentLinkedQueue<>();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        Window(final Config config, final long[] expected, final int failAt)
        {
            this.config = config;
            this.expected = expected;
            this.failAt = failAt;
        }

        int completed()
        {
            int count = 0;
            for (final Stats stats : workers)
            {
                count += stats.completed;
            }
            return count;
        }
    }

    record Sample(double seconds, double cpuSeconds, double p99Micros, double p999Micros, long count, int activeWorkers)
    {
    }

    static final class Engine implements AutoCloseable
    {
        private final Config config;
        private final Semaphore admission;
        private final ThreadLocal<Local> local = ThreadLocal.withInitial(Local::new);
        private final List<Thread> threads = new ArrayList<>();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final List<Disruptor<Slot>> disruptors = new ArrayList<>();
        private final List<RingBuffer<Slot>> rings = new ArrayList<>();
        private ExecutorChannel executorChannel;
        private PartitionedChannel partitionedChannel;
        private QueueChannel queueChannel;
        private ThreadPoolExecutor executor;

        private static final class Slot
        {
            Message<Task> message;
        }

        Engine(final Config config) throws Exception
        {
            this.config = config;
            admission = new Semaphore(config.capacity());
            final ThreadFactory factory = task ->
            {
                final Thread thread = Thread.ofPlatform().daemon().name("channel-worker").unstarted(task);
                synchronized (threads)
                {
                    threads.add(thread);
                }
                return thread;
            };
            final MessageHandler handler = message -> consume((Task) message.getPayload());
            final DefaultListableBeanFactory beans = new DefaultListableBeanFactory();
            switch (config.mode())
            {
                case EXECUTOR ->
                {
                    executor = new ThreadPoolExecutor(config.workers(), config.workers(), 0, TimeUnit.SECONDS,
                            new LinkedBlockingQueue<>(config.capacity()), factory, new ThreadPoolExecutor.AbortPolicy());
                    executorChannel = new ExecutorChannel(executor);
                    executorChannel.setBeanName("comparison");
                    executorChannel.setBeanFactory(beans);
                    executorChannel.afterPropertiesSet();
                    executorChannel.subscribe(handler);
                }
                case PARTITIONED ->
                {
                    partitionedChannel = new PartitionedChannel(config.workers(), message -> message.getHeaders().get("key"));
                    partitionedChannel.setBeanName("comparison");
                    partitionedChannel.setBeanFactory(beans);
                    partitionedChannel.setWorkerQueueSize(config.capacity());
                    partitionedChannel.setThreadFactory(factory);
                    partitionedChannel.afterPropertiesSet();
                    partitionedChannel.subscribe(handler);
                }
                case QUEUE ->
                {
                    queueChannel = new QueueChannel(config.capacity());
                    queueChannel.setBeanName("comparison");
                    queueChannel.setBeanFactory(beans);
                    queueChannel.afterPropertiesSet();
                    for (int i = 0; i < config.workers(); i++)
                    {
                        factory.newThread(() ->
                        {
                            while (running.get() && !Thread.currentThread().isInterrupted())
                            {
                                final Message<?> message = queueChannel.receive(100);
                                if (message != null)
                                {
                                    handler.handleMessage(message);
                                }
                            }
                        }).start();
                    }
                }
                case DISRUPTOR ->
                {
                    for (int i = 0; i < config.workers(); i++)
                    {
                        final Disruptor<Slot> disruptor = new Disruptor<>(Slot::new, config.capacity(), factory,
                                ProducerType.SINGLE, new BlockingWaitStrategy());
                        disruptor.handleEventsWith((slot, sequence, endOfBatch) ->
                        {
                            final Message<Task> message = slot.message;
                            slot.message = null;
                            handler.handleMessage(message);
                        });
                        disruptors.add(disruptor);
                        rings.add(disruptor.start());
                    }
                }
            }
        }

        private void consume(final Task task)
        {
            final Window window = task.window();
            final Local state = local.get();
            if (state.window != window)
            {
                state.window = window;
                state.stats = new Stats(config.keys());
                window.workers.add(state.stats);
            }
            final Stats stats = state.stats;
            try
            {
                if (Thread.currentThread() == window.producer)
                {
                    throw new IllegalStateException("Work executed on the producer thread");
                }
                if (stats.seen.get(task.id()))
                {
                    throw new IllegalStateException("Duplicate delivery");
                }
                stats.seen.set(task.id());
                if (config.contract() == Contract.KEYED)
                {
                    final int key = task.id() % config.keys();
                    if (stats.nextByKey[key] != task.id())
                    {
                        throw new IllegalStateException("Key ordering or worker affinity was violated");
                    }
                    stats.nextByKey[key] += config.keys();
                }
                if (task.id() == window.failAt)
                {
                    throw new IllegalStateException("Injected handler failure");
                }
                final long value = SpringBatchComparison.transform(new Row(task.id(), task.id()), config.work(), Failure.NONE, config.workload()).value();
                if (value != window.expected[task.id()])
                {
                    throw new IllegalStateException("Incorrect transformed value");
                }
                stats.latency.recordValue(System.nanoTime() - task.startedAt());
            }
            catch (final Throwable error)
            {
                window.failure.compareAndSet(null, error);
            }
            finally
            {
                stats.completed++;
                admission.release();
            }
        }

        private void send(final Message<Task> message)
        {
            switch (config.mode())
            {
                case EXECUTOR ->
                {
                    if (!executorChannel.send(message))
                    {
                        throw new IllegalStateException("ExecutorChannel rejected a message");
                    }
                }
                case PARTITIONED ->
                {
                    if (!partitionedChannel.send(message))
                    {
                        throw new IllegalStateException("PartitionedChannel rejected a message");
                    }
                }
                case QUEUE ->
                {
                    if (!queueChannel.send(message, 5000))
                    {
                        throw new IllegalStateException("QueueChannel admission timed out");
                    }
                }
                case DISRUPTOR ->
                {
                    final int partition = config.contract() == Contract.KEYED
                            ? Math.floorMod(message.getHeaders().get("key").hashCode(), config.workers())
                            : message.getPayload().id() % config.workers();
                    final RingBuffer<Slot> buffer = rings.get(partition);
                    final long sequence = buffer.next();
                    buffer.get(sequence).message = message;
                    buffer.publish(sequence);
                }
            }
        }

        Sample run(final long[] expected, final int failAt) throws Exception
        {
            final Window window = new Window(config, expected, failAt);
            final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
            final long cpuStarted = os.getProcessCpuTime();
            final long startedAt = System.nanoTime();
            final long deadline = startedAt + TimeUnit.SECONDS.toNanos(60);
            for (int id = 0; id < config.items(); id++)
            {
                final long beforeAdmission = System.nanoTime();
                if (beforeAdmission - deadline >= 0)
                {
                    throw new IllegalStateException("Channel run exceeded its deadline");
                }
                if (!admission.tryAcquire(5, TimeUnit.SECONDS))
                {
                    throw new IllegalStateException("Admission stalled");
                }
                final Message<Task> message = new GenericMessage<>(new Task(window, id, beforeAdmission), Map.of("key", id % config.keys()));
                try
                {
                    send(message);
                }
                catch (final Throwable error)
                {
                    admission.release();
                    throw error;
                }
            }
            while (window.completed() < config.items())
            {
                if (System.nanoTime() - deadline >= 0)
                {
                    throw new IllegalStateException("Consumers did not complete all messages");
                }
                LockSupport.parkNanos(100000);
            }
            if (!admission.tryAcquire(config.capacity(), 5, TimeUnit.SECONDS))
            {
                throw new IllegalStateException("Admission permits were not reclaimed");
            }
            admission.release(config.capacity());
            final long elapsed = System.nanoTime() - startedAt;
            final long cpu = os.getProcessCpuTime() - cpuStarted;
            if (window.failure.get() != null)
            {
                throw new IllegalStateException("Channel processing failed", window.failure.get());
            }
            final BitSet seen = new BitSet();
            final Histogram histogram = new Histogram(TimeUnit.MINUTES.toNanos(1), 3);
            for (final Stats stats : window.workers)
            {
                if (seen.intersects(stats.seen))
                {
                    throw new IllegalStateException("Duplicate delivery across workers");
                }
                seen.or(stats.seen);
                histogram.add(stats.latency);
            }
            if (seen.cardinality() != config.items() || seen.nextClearBit(0) != config.items() || histogram.getTotalCount() != config.items())
            {
                throw new IllegalStateException("Missing messages or latency samples");
            }
            if (cpuStarted < 0 || cpu < 0)
            {
                throw new UnsupportedOperationException("Process CPU time is unavailable");
            }
            return new Sample(elapsed / 1e9, cpu / 1e9, histogram.getValueAtPercentile(99) / 1e3,
                    histogram.getValueAtPercentile(99.9) / 1e3, histogram.getTotalCount(), window.workers.size());
        }

        @Override
        public void close() throws Exception
        {
            running.set(false);
            if (executorChannel != null)
            {
                executorChannel.destroy();
                executor.shutdown();
            }
            if (partitionedChannel != null)
            {
                partitionedChannel.destroy();
            }
            for (final Disruptor<Slot> disruptor : disruptors)
            {
                disruptor.halt();
            }
            final List<Thread> snapshot;
            synchronized (threads)
            {
                snapshot = List.copyOf(threads);
            }
            if (queueChannel != null)
            {
                snapshot.forEach(Thread::interrupt);
            }
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            for (final Thread thread : snapshot)
            {
                final long remaining = deadline - System.nanoTime();
                if (remaining <= 0 || !thread.join(Duration.ofNanos(remaining)))
                {
                    snapshot.forEach(Thread::interrupt);
                    if (executor != null)
                    {
                        executor.shutdownNow();
                    }
                    throw new IllegalStateException("Channel worker did not stop");
                }
            }
        }
    }

    private ChannelComparison()
    {
    }

    public static void main(final String[] args) throws Exception
    {
        if (args.length != 11)
        {
            throw new IllegalArgumentException("Expected mode contract items cpuRounds workload workers capacity keys warmups runs output.json");
        }
        final Config config = new Config(Mode.valueOf(args[0]), Contract.valueOf(args[1]), Integer.parseInt(args[2]),
                Integer.parseInt(args[3]), Workload.valueOf(args[4]), Integer.parseInt(args[5]), Integer.parseInt(args[6]), Integer.parseInt(args[7]));
        final int warmups = Integer.parseInt(args[8]);
        final int runs = Integer.parseInt(args[9]);
        if (warmups < 0 || warmups > 10 || runs < 1 || runs > 20)
        {
            throw new IllegalArgumentException("Require warmups 0..10 and runs 1..20");
        }
        final long[] expected = SpringBatchComparison.expected(config.items(), config.work(), config.workload());
        final List<String> samples = new ArrayList<>();
        try (Engine engine = new Engine(config))
        {
            for (int i = 0; i < warmups + runs; i++)
            {
                final Sample sample = engine.run(expected, -1);
                if (i >= warmups)
                {
                    samples.add(String.format(Locale.ROOT,
                            "{\"seconds\":%.6f,\"eventsPerSecond\":%.3f,\"cpuSeconds\":%.6f,\"p99Micros\":%.3f," +
                            "\"p999Micros\":%.3f,\"completed\":%d,\"activeWorkers\":%d}",
                            sample.seconds(), sample.count() / sample.seconds(), sample.cpuSeconds(), sample.p99Micros(),
                            sample.p999Micros(), sample.count(), sample.activeWorkers()));
                }
            }
        }
        final String result = String.format(Locale.ROOT,
                "{\"mode\":\"%s\",\"contract\":\"%s\",\"items\":%d,\"cpuRounds\":%d,\"workload\":\"%s\"," +
                "\"workers\":%d,\"capacity\":%d,\"keys\":%d,\"warmups\":%d,\"runs\":%d," +
                "\"springIntegration\":\"%s\",\"jdk\":\"%s\",\"os\":\"%s\",\"arch\":\"%s\"," +
                "\"availableProcessors\":%d,\"samples\":[%s]}",
                config.mode(), config.contract(), config.items(), config.work(), config.workload(), config.workers(), config.capacity(), config.keys(),
                warmups, runs, ExecutorChannel.class.getPackage().getImplementationVersion(), Runtime.version(), System.getProperty("os.name"),
                System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors(), String.join(",", samples));
        final Path output = Path.of(args[10]).toAbsolutePath();
        Files.createDirectories(output.getParent());
        Files.writeString(output, result + System.lineSeparator());
        System.out.println(result);
    }
}

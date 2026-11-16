package com.lmax.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.sun.management.OperatingSystemMXBean;
import org.HdrHistogram.Histogram;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public final class ThreadModelComparison
{
    private static final int BUFFER_SIZE = 1024;

    private ThreadModelComparison()
    {
    }

    private enum ThreadKind
    {
        PLATFORM, VIRTUAL;

        ThreadFactory factory()
        {
            return this == VIRTUAL ? Thread.ofVirtual().factory() : Thread.ofPlatform().daemon().factory();
        }
    }

    private enum Waiting
    {
        BUSY_SPIN, BLOCKING;

        WaitStrategy strategy()
        {
            return this == BUSY_SPIN ? new BusySpinWaitStrategy() : new BlockingWaitStrategy();
        }
    }

    private enum Workload
    {
        CPU, WAIT
    }

    private static final class Event
    {
        long startedAt;
        long value;
        Window window;
    }

    private static final class Samples
    {
        final Histogram latency = new Histogram(TimeUnit.MINUTES.toNanos(5), 3);
        long published;
        long batches;
        long completed;
    }

    private static final class Window
    {
        final Samples[] streams;
        final long startedAt;
        final long deadline;

        Window(final int streamCount, final int seconds)
        {
            streams = new Samples[streamCount];
            for (int i = 0; i < streams.length; i++)
            {
                streams[i] = new Samples();
            }
            startedAt = System.nanoTime();
            deadline = startedAt + TimeUnit.SECONDS.toNanos(seconds);
        }
    }

    private static final class Stream
    {
        final Semaphore capacity;
        final Disruptor<Event> disruptor;
        long lastSequence = -1;
        long checksum;

        Stream(final int inFlight, final ThreadFactory factory, final Waiting waiting)
        {
            capacity = new Semaphore(inFlight);
            disruptor = new Disruptor<>(Event::new, BUFFER_SIZE, factory, ProducerType.SINGLE, waiting.strategy());
        }
    }

    public static void main(final String[] args) throws Exception
    {
        if (args.length != 9 && args.length != 11)
        {
            throw new IllegalArgumentException("Expected producer consumer wait workload streams inFlight warmupSeconds measureSeconds output.json [batchSize handlerWork]");
        }
        final ThreadKind producer = ThreadKind.valueOf(args[0]);
        final ThreadKind consumer = ThreadKind.valueOf(args[1]);
        final Waiting waiting = Waiting.valueOf(args[2]);
        final Workload workload = Workload.valueOf(args[3]);
        final int streamCount = Integer.parseInt(args[4]);
        final int inFlight = Integer.parseInt(args[5]);
        final int warmupSeconds = Integer.parseInt(args[6]);
        final int measureSeconds = Integer.parseInt(args[7]);
        final int batchSize = args.length == 11 ? Integer.parseInt(args[9]) : 1;
        final int handlerWork = args.length == 11 ? Integer.parseInt(args[10]) : 64;
        if (streamCount < 1 || inFlight < 1 || inFlight > BUFFER_SIZE / 4 ||
                warmupSeconds < 1 || warmupSeconds > 60 || measureSeconds < 1 || measureSeconds > 120)
        {
            throw new IllegalArgumentException("Require streams > 0, inFlight 1..256, warmupSeconds 1..60 and measureSeconds 1..120");
        }
        if (batchSize < 1 || batchSize > inFlight || handlerWork < 0 || handlerWork > 100000)
        {
            throw new IllegalArgumentException("Require batchSize 1..inFlight and handlerWork 0..100000");
        }

        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicReference<Throwable> failure = new AtomicReference<>();
        final AtomicReference<Window> currentWindow = new AtomicReference<>();
        final CountDownLatch ready = new CountDownLatch(streamCount * 2);
        final CountDownLatch start = new CountDownLatch(1);
        final List<Thread> threads = new ArrayList<>();
        final List<Stream> streams = new ArrayList<>();
        final ThreadFactory producers = trackedFactory(producer.factory(), ready, start, threads, running, failure);
        final ThreadFactory consumers = trackedFactory(consumer.factory(), ready, start, threads, running, failure);
        final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        Window measured = null;
        long cpuNanos = 0;
        long cpuSampleNanos = 0;
        try
        {
            for (int i = 0; i < streamCount; i++)
            {
                final int streamIndex = i;
                final Stream stream = new Stream(inFlight, consumers, waiting);
                streams.add(stream);
                stream.disruptor.handleEventsWith((event, sequence, endOfBatch) ->
                        consume(stream, streamIndex, workload, handlerWork, event, sequence));
                stream.disruptor.start();
                producers.newThread(() -> produce(stream, streamIndex, batchSize, currentWindow, running)).start();
            }
            if (!ready.await(10, TimeUnit.SECONDS))
            {
                throw new IllegalStateException("Workers did not reach the start barrier");
            }
            final Window warmup = new Window(streamCount, warmupSeconds);
            currentWindow.set(warmup);
            start.countDown();
            awaitDeadline(warmup.deadline);

            measured = new Window(streamCount, measureSeconds);
            final long cpuStarted = os.getProcessCpuTime();
            final long cpuSampleStarted = System.nanoTime();
            currentWindow.set(measured);
            awaitDeadline(measured.deadline);
            cpuNanos = os.getProcessCpuTime() - cpuStarted;
            cpuSampleNanos = System.nanoTime() - cpuSampleStarted;
            if (cpuStarted < 0 || cpuNanos < 0)
            {
                throw new UnsupportedOperationException("Process CPU time is unavailable");
            }
        }
        finally
        {
            running.set(false);
            start.countDown();
            for (final Stream stream : streams)
            {
                stream.disruptor.halt();
            }
            final long shutdownDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            for (final Thread thread : threads)
            {
                final long remaining = shutdownDeadline - System.nanoTime();
                if (remaining > 0)
                {
                    thread.join(Duration.ofNanos(remaining));
                }
                if (thread.isAlive())
                {
                    thread.interrupt();
                    failure.compareAndSet(null, new IllegalStateException("Worker did not stop before the shutdown deadline"));
                }
            }
        }
        if (failure.get() != null)
        {
            throw new IllegalStateException("Thread-model comparison failed", failure.get());
        }
        final String result = result(producer, consumer, waiting, workload, inFlight, batchSize, handlerWork, warmupSeconds,
                measured, streams, threads, cpuNanos, cpuSampleNanos);
        final Path output = Path.of(args[8]).toAbsolutePath();
        Files.createDirectories(output.getParent());
        Files.writeString(output, result + System.lineSeparator());
        System.out.println(result);
    }

    private static ThreadFactory trackedFactory(
            final ThreadFactory factory, final CountDownLatch ready, final CountDownLatch start,
            final List<Thread> threads, final AtomicBoolean running, final AtomicReference<Throwable> failure)
    {
        return task ->
        {
            final Thread thread = factory.newThread(() ->
            {
                try
                {
                    ready.countDown();
                    start.await();
                    if (running.get())
                    {
                        task.run();
                    }
                }
                catch (final Throwable e)
                {
                    failure.compareAndSet(null, e);
                }
            });
            threads.add(thread);
            return thread;
        };
    }

    private static void produce(
            final Stream stream, final int streamIndex, final int batchSize, final AtomicReference<Window> currentWindow,
            final AtomicBoolean running)
    {
        try
        {
            final RingBuffer<Event> buffer = stream.disruptor.getRingBuffer();
            while (running.get())
            {
                if (stream.capacity.tryAcquire(100, TimeUnit.MILLISECONDS))
                {
                    int claimed = 1;
                    if (batchSize > 1)
                    {
                        final int extra = Math.min(batchSize - 1, stream.capacity.availablePermits());
                        if (extra > 0 && stream.capacity.tryAcquire(extra))
                        {
                            claimed += extra;
                        }
                    }
                    final Window window = currentWindow.get();
                    final long startedAt = System.nanoTime();
                    final long high = buffer.next(claimed);
                    final long low = high - claimed + 1;
                    for (long sequence = low; sequence <= high; sequence++)
                    {
                        final Event event = buffer.get(sequence);
                        event.startedAt = startedAt;
                        event.value = sequence;
                        event.window = window;
                    }
                    if (startedAt - window.deadline < 0)
                    {
                        window.streams[streamIndex].published += claimed;
                        window.streams[streamIndex].batches++;
                    }
                    buffer.publish(low, high);
                }
            }
        }
        catch (final InterruptedException e)
        {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Producer interrupted", e);
        }
    }

    private static void consume(
            final Stream stream, final int streamIndex, final Workload workload, final int handlerWork,
            final Event event, final long sequence)
            throws InterruptedException
    {
        try
        {
            if (event.value != sequence || sequence != stream.lastSequence + 1)
            {
                throw new IllegalStateException("Event payload or sequence was corrupted");
            }
            stream.lastSequence = sequence;
            long value = event.value;
            if (workload == Workload.WAIT)
            {
                Thread.sleep(Duration.ofMillis(1));
            }
            else
            {
                for (int i = 0; i < handlerWork; i++)
                {
                    value = Long.rotateLeft(value ^ 0x9E3779B97F4A7C15L, 7) * 0xD1342543DE82EF95L;
                }
            }
            stream.checksum += value;
            final long completedAt = System.nanoTime();
            final Window window = event.window;
            if (completedAt - window.deadline < 0)
            {
                final Samples samples = window.streams[streamIndex];
                samples.latency.recordValue(completedAt - event.startedAt);
                samples.completed++;
            }
        }
        finally
        {
            stream.capacity.release();
        }
    }

    private static void awaitDeadline(final long deadline) throws InterruptedException
    {
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0)
        {
            Thread.sleep(Duration.ofNanos(remaining));
        }
    }

    private static String result(
            final ThreadKind producer, final ThreadKind consumer, final Waiting waiting, final Workload workload,
            final int inFlight, final int batchSize, final int handlerWork, final int warmupSeconds,
            final Window measured, final List<Stream> streams,
            final List<Thread> threads, final long cpuNanos, final long cpuSampleNanos)
    {
        final Histogram histogram = new Histogram(TimeUnit.MINUTES.toNanos(5), 3);
        long published = 0;
        long batches = 0;
        long completed = 0;
        long checksum = 0;
        int stalled = 0;
        final List<Long> counts = new ArrayList<>();
        for (int i = 0; i < measured.streams.length; i++)
        {
            final Samples samples = measured.streams[i];
            if (samples.latency.getTotalCount() != samples.completed)
            {
                throw new IllegalStateException("Latency sample count does not match completed events");
            }
            if (samples.completed > samples.published)
            {
                throw new IllegalStateException("Completed events exceed admitted events");
            }
            histogram.add(samples.latency);
            published += samples.published;
            batches += samples.batches;
            completed += samples.completed;
            counts.add(samples.completed);
            checksum += streams.get(i).checksum;
            if (samples.completed == 0)
            {
                stalled++;
            }
        }
        final String status = completed == 0 ? "NO_PROGRESS" : stalled == 0 ? "OK" : "PARTIAL_PROGRESS";
        final double seconds = (measured.deadline - measured.startedAt) / 1e9;
        final String p99 = completed == 0 ? "null" : String.format(Locale.ROOT, "%.3f", histogram.getValueAtPercentile(99) / 1e3);
        final long virtualWorkers = threads.stream().filter(Thread::isVirtual).count();
        return String.format(Locale.ROOT,
                "{\"producer\":\"%s\",\"consumer\":\"%s\",\"wait\":\"%s\",\"workload\":\"%s\"," +
                "\"streams\":%d,\"inFlight\":%d,\"batchSize\":%d,\"handlerWork\":%d,\"warmupSeconds\":%d,\"measureSeconds\":%.3f," +
                "\"status\":\"%s\",\"eventsPerSecond\":%.3f,\"p99Micros\":%s,\"cpuCores\":%.3f,\"cpuSampleSeconds\":%.3f," +
                "\"published\":%d,\"completed\":%d,\"censored\":%d,\"stalledStreams\":%d,\"averagePublicationBatchSize\":%.3f," +
                "\"perStreamCompleted\":%s,\"checksum\":%d,\"availableProcessors\":%d," +
                "\"virtualWorkers\":%d,\"platformWorkers\":%d,\"schedulerParallelism\":%d," +
                "\"assertions\":%b,\"jdk\":\"%s\"}",
                producer, consumer, waiting, workload, streams.size(), inFlight, batchSize, handlerWork, warmupSeconds, seconds,
                status, completed / seconds, p99, (double) cpuNanos / cpuSampleNanos, cpuSampleNanos / 1e9,
                published, completed, published - completed, stalled, batches == 0 ? 0.0 : (double) published / batches, counts, checksum,
                Runtime.getRuntime().availableProcessors(), virtualWorkers, threads.size() - virtualWorkers,
                Integer.getInteger("jdk.virtualThreadScheduler.parallelism", Runtime.getRuntime().availableProcessors()),
                SingleProducerSequencer.class.desiredAssertionStatus(), Runtime.version());
    }
}

package com.lmax.disruptor.batchcompare;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.sun.management.OperatingSystemMXBean;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.configuration.support.MapJobRegistry;
import org.springframework.batch.core.job.JobExecution;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.parameters.JobParameters;
import org.springframework.batch.core.job.parameters.JobParametersBuilder;
import org.springframework.batch.core.launch.support.TaskExecutorJobOperator;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JdbcJobRepositoryFactoryBean;
import org.springframework.batch.core.step.Step;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.infrastructure.item.Chunk;
import org.springframework.batch.infrastructure.item.support.AbstractItemCountingItemStreamItemReader;
import org.springframework.core.SpringVersion;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.task.SyncTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.lang.management.ManagementFactory;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Comparator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class SpringBatchComparison
{
    enum Mode
    {
        SPRING, SPRING_PARALLEL, DISRUPTOR
    }

    enum Workload
    {
        ARITHMETIC, SHA256
    }

    enum Input
    {
        GENERATED, CSV
    }

    enum Storage
    {
        MEMORY, FILE
    }

    enum Waiting
    {
        BLOCKING, BUSY_SPIN
    }

    record Options(int workers, Workload workload, Input input, Storage storage, Waiting waiting, int ringSize)
    {
        static final Options DEFAULT = new Options(1, Workload.ARITHMETIC, Input.GENERATED, Storage.MEMORY, Waiting.BLOCKING, 1024);

        Options
        {
            if (workers < 1 || workers > 16 || ringSize < 1 || ringSize > 65536 || Integer.bitCount(ringSize) != 1)
            {
                throw new IllegalArgumentException("Require workers 1..16 and a power-of-two ringSize 1..65536");
            }
        }
    }

    static final class Dataset implements AutoCloseable
    {
        final Path directory;
        final Path input;

        Dataset(final Options options, final int items) throws IOException
        {
            directory = Files.createTempDirectory("batch-comparison-");
            input = options.input() == Input.CSV ? directory.resolve("input.csv") : null;
            if (input != null)
            {
                try (var writer = Files.newBufferedWriter(input))
                {
                    for (int i = 0; i < items; i++)
                    {
                        writer.write(i + "," + i);
                        writer.newLine();
                    }
                }
            }
        }

        @Override
        public void close() throws IOException
        {
            try (var paths = Files.walk(directory))
            {
                for (final Path path : paths.sorted(Comparator.reverseOrder()).toList())
                {
                    Files.delete(path);
                }
            }
        }
    }

    enum FailurePoint
    {
        NONE, PROCESSOR, WRITER
    }

    record Failure(FailurePoint point, int id)
    {
        static final Failure NONE = new Failure(FailurePoint.NONE, -1);
    }

    record Row(int id, long value)
    {
    }

    private SpringBatchComparison()
    {
    }

    private static final ThreadLocal<MessageDigest> DIGESTS = ThreadLocal.withInitial(() ->
    {
        try
        {
            return MessageDigest.getInstance("SHA-256");
        }
        catch (final NoSuchAlgorithmException error)
        {
            throw new IllegalStateException(error);
        }
    });

    static Row transform(final Row row, final int work, final Failure failure)
    {
        return transform(row, work, failure, Workload.ARITHMETIC);
    }

    static Row transform(final Row row, final int work, final Failure failure, final Workload workload)
    {
        if (failure.point() == FailurePoint.PROCESSOR && row.id() == failure.id())
        {
            throw new IllegalStateException("Injected processor failure at " + row.id());
        }
        long value = row.value();
        if (workload == Workload.SHA256)
        {
            byte[] bytes = ByteBuffer.allocate(16).putLong(row.id()).putLong(value).array();
            final MessageDigest digest = DIGESTS.get();
            for (int i = 0; i < work; i++)
            {
                bytes = digest.digest(bytes);
            }
            return new Row(row.id(), ByteBuffer.wrap(bytes).getLong());
        }
        for (int i = 0; i < work; i++)
        {
            value = Long.rotateLeft(value ^ 0x9E3779B97F4A7C15L, 7) * 0xD1342543DE82EF95L;
        }
        return new Row(row.id(), value);
    }

    static final class Reader extends AbstractItemCountingItemStreamItemReader<Row>
    {
        private final int items;
        private int cursor;
        private final Path path;
        private BufferedReader input;

        Reader(final int items)
        {
            this(items, null);
        }

        Reader(final int items, final Path path)
        {
            this.items = items;
            this.path = path;
            setName("input");
        }

        @Override
        protected Row doRead() throws IOException
        {
            if (cursor >= items)
            {
                return null;
            }
            final int id = cursor++;
            if (input != null)
            {
                final String line = input.readLine();
                if (line == null)
                {
                    throw new IOException("Input ended before row " + id);
                }
                final String[] fields = line.split(",");
                if (fields.length != 2 || Integer.parseInt(fields[0]) != id)
                {
                    throw new IOException("Invalid CSV row " + id);
                }
                return new Row(id, Long.parseLong(fields[1]));
            }
            return new Row(id, id);
        }

        @Override
        protected void doOpen() throws IOException
        {
            cursor = 0;
            input = path == null ? null : Files.newBufferedReader(path);
        }

        @Override
        protected void doClose() throws IOException
        {
            if (input != null)
            {
                input.close();
                input = null;
            }
            cursor = 0;
        }

        @Override
        protected void jumpToItem(final int itemIndex) throws IOException
        {
            if (input != null)
            {
                for (int i = 0; i < itemIndex; i++)
                {
                    if (input.readLine() == null)
                    {
                        throw new IOException("Checkpoint is beyond CSV input");
                    }
                }
            }
            cursor = itemIndex;
        }
    }

    static final class Database implements AutoCloseable
    {
        private static final AtomicInteger IDS = new AtomicInteger();
        final JdbcTemplate jdbc;
        final JdbcTransactionManager transactions;
        final JobRepository repository;
        final TaskExecutorJobOperator operator;
        final MapJobRegistry registry = new MapJobRegistry();
        private final Options options;
        private ThreadPoolTaskExecutor executor;

        Database() throws Exception
        {
            this(Options.DEFAULT, null);
        }

        Database(final Options options, final Path directory) throws Exception
        {
            this.options = options;
            final String url = options.storage() == Storage.MEMORY
                    ? "jdbc:h2:mem:comparison_" + IDS.getAndIncrement() + ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE"
                    : "jdbc:h2:file:" + directory.resolve("output").toAbsolutePath() + ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE;WRITE_DELAY=0";
            final DriverManagerDataSource source = new DriverManagerDataSource(
                    url, "sa", "");
            new ResourceDatabasePopulator(new ClassPathResource("org/springframework/batch/core/schema-h2.sql")).execute(source);
            jdbc = new JdbcTemplate(source);
            jdbc.execute("CREATE TABLE results (id INT PRIMARY KEY, result_value BIGINT NOT NULL)");
            transactions = new JdbcTransactionManager(source);
            final JdbcJobRepositoryFactoryBean factory = new JdbcJobRepositoryFactoryBean();
            factory.setDataSource(source);
            factory.setTransactionManager(transactions);
            factory.afterPropertiesSet();
            repository = factory.getObject();
            operator = new TaskExecutorJobOperator();
            operator.setJobRepository(repository);
            operator.setJobRegistry(registry);
            operator.setTaskExecutor(new SyncTaskExecutor());
            operator.afterPropertiesSet();
        }

        Job job(final Mode mode, final int items, final int work, final int chunkSize,
                final Failure failure, final Pipeline pipeline) throws Exception
        {
            return job(mode, items, work, chunkSize, failure, pipeline, null);
        }

        Job job(final Mode mode, final int items, final int work, final int chunkSize,
                final Failure failure, final Pipeline pipeline, final Path input) throws Exception
        {
            if (options.input() == Input.CSV && input == null)
            {
                throw new IllegalArgumentException("CSV input path is required");
            }
            final var builder = new StepBuilder("transform-and-store", repository)
                    .<Row, Row>chunk(chunkSize)
                    .transactionManager(transactions)
                    .reader(new Reader(items, input))
                    .processor(row -> mode != Mode.DISRUPTOR ? transform(row, work, failure, options.workload()) : row)
                    .writer(chunk -> write(mode != Mode.DISRUPTOR ? chunk : pipeline.transform(chunk, work, failure), failure));
            if (mode == Mode.SPRING_PARALLEL)
            {
                if (executor == null)
                {
                    executor = new ThreadPoolTaskExecutor();
                    executor.setCorePoolSize(options.workers());
                    executor.setMaxPoolSize(options.workers());
                    executor.setThreadNamePrefix("spring-cpu-");
                    executor.setDaemon(true);
                    executor.setWaitForTasksToCompleteOnShutdown(true);
                    executor.initialize();
                }
                builder.taskExecutor(executor);
            }
            final Step step = builder.build();
            final Job job = new JobBuilder("comparison", repository).start(step).build();
            registry.unregister(job.getName());
            registry.register(job);
            return job;
        }

        void write(final Chunk<? extends Row> chunk, final Failure failure)
        {
            if (!TransactionSynchronizationManager.isActualTransactionActive())
            {
                throw new IllegalStateException("Writer is outside the chunk transaction");
            }
            jdbc.batchUpdate("INSERT INTO results (id, result_value) VALUES (?, ?)", chunk.getItems(), chunk.size(), (statement, row) ->
            {
                statement.setInt(1, row.id());
                statement.setLong(2, row.value());
            });
            if (failure.point() == FailurePoint.WRITER && chunk.getItems().stream().anyMatch(row -> row.id() == failure.id()))
            {
                throw new IllegalStateException("Injected writer failure after SQL insert");
            }
        }

        long validate(final long[] expected, final int count)
        {
            return jdbc.query("SELECT id, result_value FROM results ORDER BY id", result ->
            {
                int rows = 0;
                long checksum = 0;
                while (result.next())
                {
                    final int id = result.getInt(1);
                    final long value = result.getLong(2);
                    if (rows >= count || id != rows || value != expected[id])
                    {
                        throw new IllegalStateException("Missing, duplicate, unordered or corrupted output at row " + rows);
                    }
                    checksum += value;
                    rows++;
                }
                if (rows != count)
                {
                    throw new IllegalStateException("Expected " + count + " rows, got " + rows);
                }
                return checksum;
            });
        }

        @Override
        public void close() throws Exception
        {
            try
            {
                if (executor != null)
                {
                    executor.shutdown();
                    if (!executor.getThreadPoolExecutor().awaitTermination(5, TimeUnit.SECONDS))
                    {
                        executor.getThreadPoolExecutor().shutdownNow();
                        throw new IllegalStateException("Spring CPU workers did not stop");
                    }
                }
            }
            finally
            {
                jdbc.execute("SHUTDOWN");
            }
        }
    }

    static final class Pipeline implements AutoCloseable
    {
        private final Options options;
        private final List<Worker> workers = new ArrayList<>();

        private static final class Batch
        {
            final Row[] results;
            final CountDownLatch completed;
            final AtomicReference<Throwable> failure = new AtomicReference<>();
            final int work;
            final Failure injected;

            Batch(final int size, final int participants, final int work, final Failure injected)
            {
                results = new Row[size];
                completed = new CountDownLatch(participants);
                this.work = work;
                this.injected = injected;
            }
        }

        private static final class Event
        {
            Row row;
            Batch batch;
            int index;
            boolean last;
        }

        private final class Worker implements AutoCloseable
        {
            private final Disruptor<Event> disruptor;
            private final RingBuffer<Event> buffer;
            private final Thread consumer;

            Worker(final int index)
            {
                final AtomicReference<Thread> thread = new AtomicReference<>();
                disruptor = new Disruptor<>(Event::new, options.ringSize(), task ->
                {
                    final Thread created = Thread.ofPlatform().daemon().name("batch-cpu-" + index).unstarted(task);
                    thread.set(created);
                    return created;
                }, ProducerType.SINGLE, options.waiting() == Waiting.BLOCKING
                        ? new BlockingWaitStrategy() : new BusySpinWaitStrategy());
                disruptor.handleEventsWith((event, sequence, endOfBatch) ->
                {
                    final Batch batch = event.batch;
                    try
                    {
                        if (event.index == 0 && TransactionSynchronizationManager.isActualTransactionActive())
                        {
                            throw new IllegalStateException("CPU worker unexpectedly owns the writer transaction");
                        }
                        if (batch.failure.get() == null)
                        {
                            batch.results[event.index] = SpringBatchComparison.transform(
                                    event.row, batch.work, batch.injected, options.workload());
                        }
                    }
                    catch (final Throwable error)
                    {
                        batch.failure.compareAndSet(null, error);
                    }
                    finally
                    {
                        event.row = null;
                        event.batch = null;
                        if (event.last)
                        {
                            batch.completed.countDown();
                        }
                    }
                });
                buffer = disruptor.start();
                consumer = thread.get();
            }

            @Override
            public void close() throws Exception
            {
                try
                {
                    disruptor.shutdown(5, TimeUnit.SECONDS);
                }
                finally
                {
                    disruptor.halt();
                    if (!consumer.join(Duration.ofSeconds(5)))
                    {
                        consumer.interrupt();
                        throw new IllegalStateException("CPU consumer did not stop");
                    }
                }
            }
        }

        Pipeline()
        {
            this(Options.DEFAULT);
        }

        Pipeline(final Options options)
        {
            this.options = options;
            for (int i = 0; i < options.workers(); i++)
            {
                workers.add(new Worker(i));
            }
        }

        Chunk<Row> transform(final Chunk<? extends Row> input, final int work, final Failure failure) throws Exception
        {
            if (input.isEmpty())
            {
                return new Chunk<>(List.of());
            }
            final int participants = Math.min(workers.size(), input.size());
            if ((input.size() + participants - 1) / participants > options.ringSize())
            {
                throw new IllegalArgumentException("Chunk partition exceeds ring capacity");
            }
            final List<? extends Row> rows = input.getItems();
            final Batch batch = new Batch(input.size(), participants, work, failure);
            for (int worker = 0; worker < participants; worker++)
            {
                final RingBuffer<Event> buffer = workers.get(worker).buffer;
                final int count = (input.size() - worker + participants - 1) / participants;
                final long high = buffer.next(count);
                final long low = high - count + 1;
                for (int i = 0; i < count; i++)
                {
                    final int index = worker + i * participants;
                    final Event event = buffer.get(low + i);
                    event.row = rows.get(index);
                    event.batch = batch;
                    event.index = index;
                    event.last = i == count - 1;
                }
                buffer.publish(low, high);
            }
            if (!batch.completed.await(30, TimeUnit.SECONDS))
            {
                throw new IllegalStateException("CPU consumers did not finish their chunk");
            }
            if (batch.failure.get() != null)
            {
                throw new IllegalStateException("CPU consumer failed", batch.failure.get());
            }
            return new Chunk<>(Arrays.asList(batch.results));
        }

        @Override
        public void close() throws Exception
        {
            Exception failure = null;
            for (final Worker worker : workers)
            {
                try
                {
                    worker.close();
                }
                catch (final Exception error)
                {
                    if (failure == null)
                    {
                        failure = error;
                    }
                    else
                    {
                        failure.addSuppressed(error);
                    }
                }
            }
            if (failure != null)
            {
                throw failure;
            }
        }
    }

    static long[] expected(final int items, final int work)
    {
        return expected(items, work, Workload.ARITHMETIC);
    }

    static long[] expected(final int items, final int work, final Workload workload)
    {
        final long[] values = new long[items];
        for (int i = 0; i < items; i++)
        {
            values[i] = transform(new Row(i, i), work, Failure.NONE, workload).value();
        }
        return values;
    }

    static JobParameters parameters(final int run)
    {
        return new JobParametersBuilder().addLong("run", (long) run).toJobParameters();
    }

    private static long gcCollections()
    {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(bean -> bean.getCollectionCount()).filter(value -> value >= 0).sum();
    }

    private static long gcMillis()
    {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(bean -> bean.getCollectionTime()).filter(value -> value >= 0).sum();
    }

    public static void main(final String[] args) throws Exception
    {
        if (args.length != 7 && args.length != 13)
        {
            throw new IllegalArgumentException("Expected mode items cpuRounds chunkSize warmupJobs measuredJobs output.json [workers workload input storage wait ringSize]");
        }
        final Mode mode = Mode.valueOf(args[0]);
        final int items = Integer.parseInt(args[1]);
        final int work = Integer.parseInt(args[2]);
        final int chunkSize = Integer.parseInt(args[3]);
        final int warmups = Integer.parseInt(args[4]);
        final int runs = Integer.parseInt(args[5]);
        final Options options = args.length == 7 ? Options.DEFAULT : new Options(Integer.parseInt(args[7]),
                Workload.valueOf(args[8]), Input.valueOf(args[9]), Storage.valueOf(args[10]), Waiting.valueOf(args[11]), Integer.parseInt(args[12]));
        if (items < 1 || items > 1000000 || work < 0 || work > 100000 || chunkSize < 1 || chunkSize > 65536 ||
                warmups < 0 || warmups > 10 || runs < 1 || runs > 20)
        {
            throw new IllegalArgumentException("Require items 1..1000000, cpuRounds 0..100000, chunkSize 1..65536, warmupJobs 0..10, measuredJobs 1..20");
        }
        if (mode == Mode.SPRING && options.workers() != 1)
        {
            throw new IllegalArgumentException("Use SPRING_PARALLEL for multiple Spring workers");
        }
        if (mode == Mode.DISRUPTOR && (chunkSize + options.workers() - 1) / options.workers() > options.ringSize())
        {
            throw new IllegalArgumentException("Chunk partition exceeds ring capacity");
        }
        final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        final long[] expected = expected(items, work, options.workload());
        final List<String> samples = new ArrayList<>();
        try (Dataset dataset = new Dataset(options, items);
                Database db = new Database(options, dataset.directory);
                Pipeline pipeline = mode == Mode.DISRUPTOR ? new Pipeline(options) : null)
        {
            for (int run = 0; run < warmups + runs; run++)
            {
                db.jdbc.update("DELETE FROM results");
                final Job job = db.job(mode, items, work, chunkSize, Failure.NONE, pipeline, dataset.input);
                final long collectionsStarted = gcCollections();
                final long gcStarted = gcMillis();
                final long cpuStarted = os.getProcessCpuTime();
                final long startedAt = System.nanoTime();
                final JobExecution execution = db.operator.start(job, parameters(run));
                final long elapsed = System.nanoTime() - startedAt;
                final long cpu = os.getProcessCpuTime() - cpuStarted;
                final long collections = gcCollections() - collectionsStarted;
                final long gc = gcMillis() - gcStarted;
                if (!execution.getExitStatus().getExitCode().equals("COMPLETED"))
                {
                    throw new IllegalStateException("Job failed: " + execution.getAllFailureExceptions());
                }
                final var step = execution.getStepExecutions().iterator().next();
                if (step.getReadCount() != items || step.getWriteCount() != items || step.getRollbackCount() != 0)
                {
                    throw new IllegalStateException("Step counts do not match the completed input");
                }
                final long checksum = db.validate(expected, items);
                if (cpuStarted < 0 || cpu < 0)
                {
                    throw new UnsupportedOperationException("Process CPU time is unavailable");
                }
                if (run >= warmups)
                {
                    samples.add(String.format(Locale.ROOT,
                            "{\"seconds\":%.6f,\"itemsPerSecond\":%.3f,\"cpuSeconds\":%.6f,\"cpuCores\":%.3f," +
                            "\"rows\":%d,\"checksum\":%d,\"commits\":%d,\"rollbacks\":%d,\"gcCollections\":%d,\"gcMillis\":%d}",
                            elapsed / 1e9, items * 1e9 / elapsed, cpu / 1e9, (double) cpu / elapsed, items, checksum,
                            step.getCommitCount(), step.getRollbackCount(), collections, gc));
                }
            }
        }
        final String result = String.format(Locale.ROOT,
                "{\"mode\":\"%s\",\"items\":%d,\"cpuRounds\":%d,\"chunkSize\":%d,\"warmupJobs\":%d," +
                "\"measuredJobs\":%d,\"springBatch\":\"%s\",\"springFramework\":\"%s\",\"jdk\":\"%s\"," +
                "\"workers\":%d,\"workload\":\"%s\",\"input\":\"%s\",\"storage\":\"%s\",\"waitStrategy\":\"%s\",\"ringSize\":%d," +
                "\"os\":\"%s\",\"arch\":\"%s\",\"availableProcessors\":%d,\"maxHeapBytes\":%d,\"samples\":[%s]}",
                mode, items, work, chunkSize, warmups, runs, Job.class.getPackage().getImplementationVersion(),
                SpringVersion.getVersion(), Runtime.version(), options.workers(), options.workload(), options.input(), options.storage(), options.waiting(), options.ringSize(),
                System.getProperty("os.name"), System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors(), Runtime.getRuntime().maxMemory(), String.join(",", samples));
        final Path output = Path.of(args[6]).toAbsolutePath();
        Files.createDirectories(output.getParent());
        Files.writeString(output, result + System.lineSeparator());
        System.out.println(result);
    }
}

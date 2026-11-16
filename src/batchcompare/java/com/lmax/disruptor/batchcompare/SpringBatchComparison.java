package com.lmax.disruptor.batchcompare;

import com.lmax.disruptor.BlockingWaitStrategy;
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
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public final class SpringBatchComparison
{
    enum Mode
    {
        SPRING, DISRUPTOR
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

    static Row transform(final Row row, final int work, final Failure failure)
    {
        if (failure.point() == FailurePoint.PROCESSOR && row.id() == failure.id())
        {
            throw new IllegalStateException("Injected processor failure at " + row.id());
        }
        long value = row.value();
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

        Reader(final int items)
        {
            this.items = items;
            setName("input");
        }

        @Override
        protected Row doRead()
        {
            if (cursor >= items)
            {
                return null;
            }
            final int id = cursor++;
            return new Row(id, id);
        }

        @Override
        protected void doOpen()
        {
            cursor = 0;
        }

        @Override
        protected void doClose()
        {
            cursor = 0;
        }

        @Override
        protected void jumpToItem(final int itemIndex)
        {
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

        Database() throws Exception
        {
            final DriverManagerDataSource source = new DriverManagerDataSource(
                    "jdbc:h2:mem:comparison_" + IDS.getAndIncrement() + ";DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE", "sa", "");
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
            final Step step = new StepBuilder("transform-and-store", repository)
                    .<Row, Row>chunk(chunkSize)
                    .transactionManager(transactions)
                    .reader(new Reader(items))
                    .processor(row -> mode == Mode.SPRING ? transform(row, work, failure) : row)
                    .writer(chunk -> write(mode == Mode.SPRING ? chunk : pipeline.transform(chunk, work, failure), failure))
                    .build();
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
        public void close()
        {
            jdbc.execute("SHUTDOWN");
        }
    }

    static final class Pipeline implements AutoCloseable
    {
        private static final int BUFFER_SIZE = 1024;
        private final Disruptor<Event> disruptor;
        private final RingBuffer<Event> buffer;
        private final Thread consumer;

        private static final class Batch
        {
            final Row[] results;
            final CountDownLatch completed = new CountDownLatch(1);
            Throwable failure;
            final int work;
            final Failure injected;

            Batch(final int size, final int work, final Failure injected)
            {
                results = new Row[size];
                this.work = work;
                this.injected = injected;
            }
        }

        private static final class Event
        {
            Row row;
            Batch batch;
            int index;
        }

        Pipeline()
        {
            final AtomicReference<Thread> thread = new AtomicReference<>();
            disruptor = new Disruptor<>(Event::new, BUFFER_SIZE, task ->
            {
                final Thread created = Thread.ofPlatform().daemon().name("batch-cpu").unstarted(task);
                thread.set(created);
                return created;
            }, ProducerType.SINGLE, new BlockingWaitStrategy());
            disruptor.handleEventsWith((event, sequence, endOfBatch) ->
            {
                final Batch batch = event.batch;
                try
                {
                    if (event.index == 0 && TransactionSynchronizationManager.isActualTransactionActive())
                    {
                        throw new IllegalStateException("CPU worker unexpectedly owns the writer transaction");
                    }
                    if (batch.failure == null)
                    {
                        batch.results[event.index] = SpringBatchComparison.transform(event.row, batch.work, batch.injected);
                    }
                }
                catch (final Throwable error)
                {
                    if (batch.failure == null)
                    {
                        batch.failure = error;
                    }
                }
                finally
                {
                    event.row = null;
                    event.batch = null;
                    if (event.index == batch.results.length - 1)
                    {
                        batch.completed.countDown();
                    }
                }
            });
            buffer = disruptor.start();
            consumer = thread.get();
        }

        Chunk<Row> transform(final Chunk<? extends Row> input, final int work, final Failure failure) throws Exception
        {
            if (input.isEmpty())
            {
                return new Chunk<>(List.of());
            }
            if (input.size() > BUFFER_SIZE)
            {
                throw new IllegalArgumentException("Chunk exceeds ring capacity");
            }
            final List<? extends Row> rows = input.getItems();
            final Batch batch = new Batch(input.size(), work, failure);
            final long high = buffer.next(input.size());
            final long low = high - input.size() + 1;
            for (int i = 0; i < input.size(); i++)
            {
                final Event event = buffer.get(low + i);
                event.row = rows.get(i);
                event.batch = batch;
                event.index = i;
            }
            buffer.publish(low, high);
            if (!batch.completed.await(30, TimeUnit.SECONDS))
            {
                throw new IllegalStateException("CPU consumer did not finish its chunk");
            }
            if (batch.failure != null)
            {
                throw new IllegalStateException("CPU consumer failed", batch.failure);
            }
            return new Chunk<>(Arrays.asList(batch.results));
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

    static long[] expected(final int items, final int work)
    {
        final long[] values = new long[items];
        for (int i = 0; i < items; i++)
        {
            values[i] = transform(new Row(i, i), work, Failure.NONE).value();
        }
        return values;
    }

    static JobParameters parameters(final int run)
    {
        return new JobParametersBuilder().addLong("run", (long) run).toJobParameters();
    }

    public static void main(final String[] args) throws Exception
    {
        if (args.length != 7)
        {
            throw new IllegalArgumentException("Expected SPRING|DISRUPTOR items cpuRounds chunkSize warmupJobs measuredJobs output.json");
        }
        final Mode mode = Mode.valueOf(args[0]);
        final int items = Integer.parseInt(args[1]);
        final int work = Integer.parseInt(args[2]);
        final int chunkSize = Integer.parseInt(args[3]);
        final int warmups = Integer.parseInt(args[4]);
        final int runs = Integer.parseInt(args[5]);
        if (items < 1 || items > 1000000 || work < 0 || work > 100000 || chunkSize < 1 || chunkSize > 1024 ||
                warmups < 0 || warmups > 10 || runs < 1 || runs > 20)
        {
            throw new IllegalArgumentException("Require items 1..1000000, cpuRounds 0..100000, chunkSize 1..1024, warmupJobs 0..10, measuredJobs 1..20");
        }
        final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        final long[] expected = expected(items, work);
        final List<String> samples = new ArrayList<>();
        try (Database db = new Database(); Pipeline pipeline = mode == Mode.DISRUPTOR ? new Pipeline() : null)
        {
            for (int run = 0; run < warmups + runs; run++)
            {
                db.jdbc.update("DELETE FROM results");
                final Job job = db.job(mode, items, work, chunkSize, Failure.NONE, pipeline);
                final long cpuStarted = os.getProcessCpuTime();
                final long startedAt = System.nanoTime();
                final JobExecution execution = db.operator.start(job, parameters(run));
                final long elapsed = System.nanoTime() - startedAt;
                final long cpu = os.getProcessCpuTime() - cpuStarted;
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
                            "\"rows\":%d,\"checksum\":%d,\"commits\":%d,\"rollbacks\":%d}",
                            elapsed / 1e9, items * 1e9 / elapsed, cpu / 1e9, (double) cpu / elapsed, items, checksum,
                            step.getCommitCount(), step.getRollbackCount()));
                }
            }
        }
        final String result = String.format(Locale.ROOT,
                "{\"mode\":\"%s\",\"items\":%d,\"cpuRounds\":%d,\"chunkSize\":%d,\"warmupJobs\":%d," +
                "\"measuredJobs\":%d,\"springBatch\":\"%s\",\"springFramework\":\"%s\",\"jdk\":\"%s\",\"samples\":[%s]}",
                mode, items, work, chunkSize, warmups, runs, Job.class.getPackage().getImplementationVersion(),
                SpringVersion.getVersion(), Runtime.version(), String.join(",", samples));
        final Path output = Path.of(args[6]).toAbsolutePath();
        Files.createDirectories(output.getParent());
        Files.writeString(output, result + System.lineSeparator());
        System.out.println(result);
    }
}

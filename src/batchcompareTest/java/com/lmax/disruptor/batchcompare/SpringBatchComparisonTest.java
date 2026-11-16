package com.lmax.disruptor.batchcompare;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.CsvSource;
import org.springframework.batch.core.job.Job;
import org.springframework.batch.core.job.JobExecution;

import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Database;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Failure;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.FailurePoint;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Mode;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Pipeline;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.expected;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.parameters;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Options;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Workload;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Input;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Storage;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Waiting;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Dataset;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Timeout(30)
class SpringBatchComparisonTest
{
    @ParameterizedTest
    @CsvSource({"SPRING, 1003, 128", "DISRUPTOR, 1003, 128", "SPRING, 1024, 1024",
            "DISRUPTOR, 1024, 1024", "SPRING, 7, 1", "DISRUPTOR, 7, 1"})
    void shouldWriteIdenticalResultsAtChunkBoundaries(final Mode mode, final int items, final int chunkSize) throws Exception
    {
        final int work = 64;
        final long[] expected = expected(items, work);
        try (Database db = new Database(); Pipeline pipeline = mode == Mode.DISRUPTOR ? new Pipeline() : null)
        {
            final Job job = db.job(mode, items, work, chunkSize, Failure.NONE, pipeline);
            final JobExecution execution = db.operator.start(job, parameters(0));
            assertEquals("COMPLETED", execution.getExitStatus().getExitCode());
            db.validate(expected, items);
            assertEquals(items, execution.getStepExecutions().iterator().next().getWriteCount());
        }
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void shouldRollbackAndRestartAfterAProcessorFailure(final Mode mode) throws Exception
    {
        verifyRestart(mode, new Failure(FailurePoint.PROCESSOR, 35));
    }

    @ParameterizedTest
    @EnumSource(Mode.class)
    void shouldRollbackInsertedRowsAndRestartAfterAWriterFailure(final Mode mode) throws Exception
    {
        verifyRestart(mode, new Failure(FailurePoint.WRITER, 35));
    }

    @ParameterizedTest
    @CsvSource({"SPRING, 1, ARITHMETIC, GENERATED, MEMORY, BLOCKING",
            "SPRING_PARALLEL, 4, ARITHMETIC, GENERATED, MEMORY, BLOCKING",
            "DISRUPTOR, 4, ARITHMETIC, GENERATED, MEMORY, BLOCKING",
            "DISRUPTOR, 4, ARITHMETIC, GENERATED, MEMORY, BUSY_SPIN",
            "SPRING_PARALLEL, 4, SHA256, CSV, FILE, BLOCKING",
            "DISRUPTOR, 4, SHA256, CSV, FILE, BLOCKING"})
    void shouldMatchResultsAcrossWorkerAndStorageConfigurations(
            final Mode mode, final int workers, final Workload workload, final Input input,
            final Storage storage, final Waiting waiting) throws Exception
    {
        final Options options = new Options(workers, workload, input, storage, waiting, 32);
        final int items = 257;
        final int work = 8;
        final long[] expected = expected(items, work, workload);
        try (Dataset dataset = new Dataset(options, items);
                Database db = new Database(options, dataset.directory);
                Pipeline pipeline = mode == Mode.DISRUPTOR ? new Pipeline(options) : null)
        {
            final Job job = db.job(mode, items, work, 32, Failure.NONE, pipeline, dataset.input);
            final JobExecution execution = db.operator.start(job, parameters(0));
            assertEquals("COMPLETED", execution.getExitStatus().getExitCode());
            db.validate(expected, items);
        }
    }

    @ParameterizedTest
    @CsvSource({"SPRING_PARALLEL, PROCESSOR", "SPRING_PARALLEL, WRITER", "DISRUPTOR, PROCESSOR", "DISRUPTOR, WRITER"})
    void shouldRestoreCsvCheckpointsWithMultipleWorkers(final Mode mode, final FailurePoint point) throws Exception
    {
        final Options options = new Options(4, Workload.SHA256, Input.CSV, Storage.FILE, Waiting.BLOCKING, 8);
        final int items = 101;
        final int work = 4;
        final long[] expected = expected(items, work, options.workload());
        try (Dataset dataset = new Dataset(options, items);
                Database db = new Database(options, dataset.directory);
                Pipeline pipeline = mode == Mode.DISRUPTOR ? new Pipeline(options) : null)
        {
            final Job failing = db.job(mode, items, work, 16, new Failure(point, 35), pipeline, dataset.input);
            final JobExecution first = db.operator.start(failing, parameters(0));
            assertEquals("FAILED", first.getExitStatus().getExitCode());
            db.validate(expected, 32);
            db.job(mode, items, work, 16, Failure.NONE, pipeline, dataset.input);
            final JobExecution restarted = db.operator.restart(first);
            assertEquals("COMPLETED", restarted.getExitStatus().getExitCode());
            assertEquals(items - 32, restarted.getStepExecutions().iterator().next().getReadCount());
            db.validate(expected, items);
        }
    }

    private void verifyRestart(final Mode mode, final Failure failure) throws Exception
    {
        final int items = 100;
        final int chunkSize = 16;
        final int committed = 32;
        final int work = 64;
        final long[] expected = expected(items, work);
        try (Database db = new Database(); Pipeline pipeline = mode == Mode.DISRUPTOR ? new Pipeline() : null)
        {
            final Job failing = db.job(mode, items, work, chunkSize, failure, pipeline);
            final JobExecution first = db.operator.start(failing, parameters(0));
            assertEquals("FAILED", first.getExitStatus().getExitCode());
            db.validate(expected, committed);

            db.job(mode, items, work, chunkSize, Failure.NONE, pipeline);
            final JobExecution restarted = db.operator.restart(first);
            assertEquals("COMPLETED", restarted.getExitStatus().getExitCode());
            assertEquals(first.getJobInstance().getId(), restarted.getJobInstance().getId());
            assertNotEquals(first.getId(), restarted.getId());
            assertEquals(items - committed, restarted.getStepExecutions().iterator().next().getReadCount());
            db.validate(expected, items);
        }
    }
}

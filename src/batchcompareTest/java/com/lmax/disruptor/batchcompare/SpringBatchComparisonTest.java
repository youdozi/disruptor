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

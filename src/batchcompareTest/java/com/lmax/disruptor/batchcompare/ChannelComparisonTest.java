package com.lmax.disruptor.batchcompare;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static com.lmax.disruptor.batchcompare.ChannelComparison.Config;
import static com.lmax.disruptor.batchcompare.ChannelComparison.Contract;
import static com.lmax.disruptor.batchcompare.ChannelComparison.Engine;
import static com.lmax.disruptor.batchcompare.ChannelComparison.Mode;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.Workload;
import static com.lmax.disruptor.batchcompare.SpringBatchComparison.expected;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Timeout(20)
class ChannelComparisonTest
{
    @ParameterizedTest
    @CsvSource({"EXECUTOR, POINT, 1", "QUEUE, POINT, 1", "DISRUPTOR, POINT, 1",
            "EXECUTOR, POINT, 4", "QUEUE, POINT, 4", "DISRUPTOR, POINT, 4",
            "PARTITIONED, KEYED, 1", "DISRUPTOR, KEYED, 1",
            "PARTITIONED, KEYED, 4", "DISRUPTOR, KEYED, 4"})
    void shouldProcessEveryMessageAndReclaimAdmission(final Mode mode, final Contract contract, final int workers) throws Exception
    {
        final Config config = new Config(mode, contract, 1003, 64, Workload.ARITHMETIC, workers, 32, 7);
        try (Engine engine = new Engine(config))
        {
            for (int i = 0; i < 3; i++)
            {
                final var sample = engine.run(expected(config.items(), config.work()), -1);
                assertEquals(config.items(), sample.count());
                assertTrue(sample.activeWorkers() <= workers);
                assertTrue(sample.seconds() > 0);
            }
        }
    }

    @ParameterizedTest
    @CsvSource({"PARTITIONED", "DISRUPTOR"})
    void shouldKeepAHotKeyOnOneWorker(final Mode mode) throws Exception
    {
        final Config config = new Config(mode, Contract.KEYED, 1003, 8, Workload.SHA256, 4, 8, 1);
        try (Engine engine = new Engine(config))
        {
            final var sample = engine.run(expected(config.items(), config.work(), config.workload()), -1);
            assertEquals(1, sample.activeWorkers());
            assertEquals(config.items(), sample.count());
        }
    }

    @ParameterizedTest
    @CsvSource({"EXECUTOR, POINT", "QUEUE, POINT", "PARTITIONED, KEYED", "DISRUPTOR, KEYED"})
    void shouldPropagateAHandlerFailureAndAllowAnotherRun(final Mode mode, final Contract contract) throws Exception
    {
        final Config config = new Config(mode, contract, 1003, 8, Workload.SHA256, 4, 32, 7);
        final long[] expected = expected(config.items(), config.work(), config.workload());
        try (Engine engine = new Engine(config))
        {
            assertThrows(IllegalStateException.class, () -> engine.run(expected, 35));
            assertEquals(config.items(), engine.run(expected, -1).count());
        }
    }
}

package com.lmax.disruptor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.List;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SingleProducerSequencerTest
{
    @ParameterizedTest
    @CsvSource({"false, 1", "true, 1", "false, 8", "true, 8", "false, 16", "true, 16", "false, 64", "true, 64"})
    public void shouldWaitForTheSlowestConsumerWhenCapacityIsExhausted(final boolean virtual, final int bufferSize)
            throws Exception
    {
        final CountDownLatch full = new CountDownLatch(1);
        final CountDownLatch stillFull = new CountDownLatch(1);
        final Sequence first = new Sequence(-1);
        final Sequence second = new Sequence(-1)
        {
            @Override
            public long get()
            {
                final long value = super.get();
                if (value == -1)
                {
                    full.countDown();
                    if (first.get() == 0)
                    {
                        stillFull.countDown();
                    }
                }
                return value;
            }
        };
        final SingleProducerSequencer sequencer = new SingleProducerSequencer(bufferSize, new BusySpinWaitStrategy());
        sequencer.addGatingSequences(first, second);
        final FutureTask<Long> producer = new FutureTask<>(() ->
        {
            final long high = sequencer.next(bufferSize);
            sequencer.publish(0, high);
            return sequencer.next();
        });
        final Thread.Builder builder = virtual ? Thread.ofVirtual() : Thread.ofPlatform().daemon();
        final Thread thread = builder.start(producer);
        try
        {
            assertTrue(full.await(5, TimeUnit.SECONDS));
            assertFalse(producer.isDone());
            first.set(0);
            assertTrue(stillFull.await(5, TimeUnit.SECONDS));
            assertFalse(producer.isDone());
            second.set(0);
            assertEquals((long) bufferSize, producer.get(5, TimeUnit.SECONDS));
        }
        finally
        {
            first.set(Long.MAX_VALUE);
            second.set(Long.MAX_VALUE);
            assertTrue(thread.join(Duration.ofSeconds(5)), "Producer did not stop");
        }
    }

    @Test
    public void shouldRejectAnotherProducerWithoutClaimingASequence() throws Exception
    {
        assertTrue(SingleProducerSequencer.class.desiredAssertionStatus());
        final SingleProducerSequencer sequencer = new SingleProducerSequencer(16, new BusySpinWaitStrategy());
        assertEquals(0, sequencer.next());

        final FutureTask<Long> secondProducer = new FutureTask<>(() -> sequencer.next());
        Thread.ofPlatform().daemon().start(secondProducer);

        final ExecutionException failure = assertThrows(ExecutionException.class, () -> secondProducer.get(5, TimeUnit.SECONDS));
        assertInstanceOf(AssertionError.class, failure.getCause());
        assertEquals(1, sequencer.next());
    }

    @Test
    public void shouldTrackProducersIndependentlyForDifferentSequencers() throws Exception
    {
        final SingleProducerSequencer first = new SingleProducerSequencer(16, new BusySpinWaitStrategy());
        final SingleProducerSequencer second = new SingleProducerSequencer(16, new BusySpinWaitStrategy());
        assertEquals(0, first.next());

        final FutureTask<Long> secondProducer = new FutureTask<>(() -> second.next());
        Thread.ofPlatform().daemon().start(secondProducer);

        assertEquals(0L, secondProducer.get(5, TimeUnit.SECONDS));
        assertEquals(1, first.next());
    }

    @RepeatedTest(25)
    public void shouldChooseOnlyOneProducerWhenFirstClaimsRace() throws Exception
    {
        assertTrue(SingleProducerSequencer.class.desiredAssertionStatus());
        final SingleProducerSequencer sequencer = new SingleProducerSequencer(16, new BusySpinWaitStrategy());
        final CountDownLatch ready = new CountDownLatch(2);
        final CountDownLatch start = new CountDownLatch(1);
        final FutureTask<Long> first = new FutureTask<>(() ->
        {
            ready.countDown();
            assertTrue(start.await(5, TimeUnit.SECONDS));
            return sequencer.next();
        });
        final FutureTask<Long> second = new FutureTask<>(() ->
        {
            ready.countDown();
            assertTrue(start.await(5, TimeUnit.SECONDS));
            return sequencer.next();
        });
        Thread.ofPlatform().daemon().start(first);
        Thread.ofPlatform().daemon().start(second);
        try
        {
            assertTrue(ready.await(5, TimeUnit.SECONDS));
        }
        finally
        {
            start.countDown();
        }

        int claims = 0;
        int rejected = 0;
        for (final FutureTask<Long> producer : List.of(first, second))
        {
            try
            {
                assertEquals(0L, producer.get(5, TimeUnit.SECONDS));
                claims++;
            }
            catch (final ExecutionException e)
            {
                assertInstanceOf(AssertionError.class, e.getCause());
                rejected++;
            }
        }
        assertEquals(1, claims);
        assertEquals(1, rejected);
    }

    @Test
    public void shouldNotUpdateCursorDuringHasAvailableCapacity() throws Exception
    {
        SingleProducerSequencer sequencer = new SingleProducerSequencer(16, new BusySpinWaitStrategy());

        for (int i = 0; i < 32; i++)
        {
            long next = sequencer.next();
            assertThat(sequencer.cursor.get(), not(next));

            sequencer.hasAvailableCapacity(13);
            assertThat(sequencer.cursor.get(), not(next));

            sequencer.publish(next);
        }
    }
}

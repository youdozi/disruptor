package com.lmax.disruptor;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.RepeatedTest;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNot.not;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class SingleProducerSequencerTest
{
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

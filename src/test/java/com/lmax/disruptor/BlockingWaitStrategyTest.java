package com.lmax.disruptor;

import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(15)
public class BlockingWaitStrategyTest
{
    private final BlockingWaitStrategy strategy = new BlockingWaitStrategy();
    private final Sequencer sequencer = new SingleProducerSequencer(16, strategy);
    private final SequenceBarrier barrier = sequencer.newBarrier();

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldWakeAfterIdleSignals(final boolean virtual) throws Exception
    {
        for (int i = 0; i < 100; i++)
        {
            strategy.signalAllWhenBlocking();
        }
        try (Waiter waiter = new Waiter(() -> barrier.waitFor(0), virtual))
        {
            waiter.awaitWaiting();
            sequencer.publish(0);
            assertEquals(0L, waiter.result.get(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldWakeAllRegisteredConsumers(final boolean virtual) throws Exception
    {
        try (Waiter first = new Waiter(() -> barrier.waitFor(0), virtual);
             Waiter second = new Waiter(() -> barrier.waitFor(0), virtual);
             Waiter third = new Waiter(() -> barrier.waitFor(0), virtual))
        {
            first.awaitWaiting();
            second.awaitWaiting();
            third.awaitWaiting();
            sequencer.publish(0);
            assertEquals(0L, first.result.get(5, TimeUnit.SECONDS));
            assertEquals(0L, second.result.get(5, TimeUnit.SECONDS));
            assertEquals(0L, third.result.get(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldRearmConsumersWaitingForLaterSequences(final boolean virtual) throws Exception
    {
        try (Waiter first = new Waiter(() -> barrier.waitFor(0), virtual);
             Waiter second = new Waiter(() -> barrier.waitFor(1), virtual);
             Waiter third = new Waiter(() -> barrier.waitFor(2), virtual))
        {
            first.awaitWaiting();
            second.awaitWaiting();
            third.awaitWaiting();
            sequencer.publish(0);
            assertEquals(0L, first.result.get(5, TimeUnit.SECONDS));
            assertFalse(second.result.isDone());
            assertFalse(third.result.isDone());
            sequencer.publish(1);
            assertEquals(1L, second.result.get(5, TimeUnit.SECONDS));
            assertFalse(third.result.isDone());
            sequencer.publish(2);
            assertEquals(2L, third.result.get(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldKeepOtherWaitersRegisteredAfterAnInterruption(final boolean virtual) throws Exception
    {
        try (Waiter first = new Waiter(() -> barrier.waitFor(0), virtual);
             Waiter second = new Waiter(() -> barrier.waitFor(0), virtual))
        {
            first.awaitWaiting();
            second.awaitWaiting();
            first.thread.interrupt();
            final ExecutionException error = assertThrows(ExecutionException.class, () -> first.result.get(5, TimeUnit.SECONDS));
            assertInstanceOf(InterruptedException.class, error.getCause());
            sequencer.publish(0);
            assertEquals(0L, second.result.get(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldRegisterAgainAfterAnInterruptedWait(final boolean virtual) throws Exception
    {
        try (Waiter first = new Waiter(() -> barrier.waitFor(0), virtual))
        {
            first.awaitWaiting();
            first.thread.interrupt();
            final ExecutionException error = assertThrows(ExecutionException.class, () -> first.result.get(5, TimeUnit.SECONDS));
            assertInstanceOf(InterruptedException.class, error.getCause());
        }
        try (Waiter next = new Waiter(() -> barrier.waitFor(0), virtual))
        {
            next.awaitWaiting();
            sequencer.publish(0);
            assertEquals(0L, next.result.get(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldWakeOnAlertAndAllowAnotherWaitAfterClear(final boolean virtual) throws Exception
    {
        try (Waiter first = new Waiter(() -> barrier.waitFor(0), virtual))
        {
            first.awaitWaiting();
            barrier.alert();
            final ExecutionException error = assertThrows(ExecutionException.class, () -> first.result.get(5, TimeUnit.SECONDS));
            assertInstanceOf(AlertException.class, error.getCause());
        }
        barrier.clearAlert();
        try (Waiter next = new Waiter(() -> barrier.waitFor(0), virtual))
        {
            next.awaitWaiting();
            sequencer.publish(0);
            assertEquals(0L, next.result.get(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldRecheckTheCursorAfterASignalWithoutPublication(final boolean virtual) throws Exception
    {
        try (Waiter waiter = new Waiter(() -> barrier.waitFor(0), virtual))
        {
            waiter.awaitWaiting();
            strategy.signalAllWhenBlocking();
            assertFalse(waiter.result.isDone());
            sequencer.publish(0);
            assertEquals(0L, waiter.result.get(5, TimeUnit.SECONDS));
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldNotMissPublicationWhileRegistering(final boolean virtual) throws Exception
    {
        for (long sequence = 0; sequence < 100; sequence++)
        {
            final long expected = sequence;
            try (Waiter waiter = new Waiter(() -> barrier.waitFor(expected), virtual))
            {
                sequencer.publish(expected);
                assertEquals(expected, waiter.result.get(5, TimeUnit.SECONDS));
            }
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void shouldObserveDependencyProgressWithoutAnotherSignal(final boolean virtual) throws Exception
    {
        final CountDownLatch observed = new CountDownLatch(1);
        final Sequence dependency = new Sequence(-1)
        {
            @Override
            public long get()
            {
                final long value = super.get();
                observed.countDown();
                return value;
            }
        };
        final SequenceBarrier dependentBarrier = sequencer.newBarrier(dependency);
        sequencer.publish(0);
        try (Waiter waiter = new Waiter(() -> dependentBarrier.waitFor(0), virtual))
        {
            assertTrue(observed.await(5, TimeUnit.SECONDS));
            assertFalse(waiter.result.isDone());
            dependency.set(0);
            assertEquals(0L, waiter.result.get(5, TimeUnit.SECONDS));
        }
    }

    private static final class Waiter implements AutoCloseable
    {
        private final FutureTask<Long> result;
        private final Thread thread;

        private Waiter(final Callable<Long> action, final boolean virtual)
        {
            result = new FutureTask<>(action);
            final Thread.Builder builder = virtual ? Thread.ofVirtual() : Thread.ofPlatform().daemon();
            thread = builder.start(result);
        }

        private void awaitWaiting()
        {
            final long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (thread.getState() != Thread.State.WAITING)
            {
                assertTrue(thread.isAlive(), "Waiter exited before entering Object.wait");
                assertTrue(System.nanoTime() - deadline < 0, "Waiter did not enter Object.wait");
                LockSupport.parkNanos(100_000);
            }
        }

        @Override
        public void close() throws InterruptedException
        {
            thread.interrupt();
            assertTrue(thread.join(Duration.ofSeconds(5)), "Waiter did not stop");
        }
    }
}

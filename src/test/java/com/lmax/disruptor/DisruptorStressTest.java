package com.lmax.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static java.lang.Math.max;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DisruptorStressTest
{
    private final ExecutorService executor = Executors.newCachedThreadPool(DaemonThreadFactory.INSTANCE);

    @ParameterizedTest
    @MethodSource("waitStrategies")
    @Timeout(30)
    public void shouldHandleLotsOfThreads(final WaitStrategy waitStrategy) throws Exception
    {
        Disruptor<TestEvent> disruptor = new Disruptor<>(
                TestEvent.FACTORY, 1 << 16, DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI, waitStrategy);
        RingBuffer<TestEvent> ringBuffer = disruptor.getRingBuffer();
        disruptor.setDefaultExceptionHandler(new FatalExceptionHandler());

        int threads = max(1, Runtime.getRuntime().availableProcessors() / 2);

        int iterations = 200000;
        int publisherCount = threads;
        int handlerCount = threads;

        CyclicBarrier barrier = new CyclicBarrier(publisherCount);
        CountDownLatch latch = new CountDownLatch(publisherCount);

        TestEventHandler[] handlers = initialise(disruptor, new TestEventHandler[handlerCount]);
        Publisher[] publishers = initialise(new Publisher[publisherCount], ringBuffer, iterations, barrier, latch);

        try
        {
            disruptor.start();

            for (Publisher publisher : publishers)
            {
                executor.execute(publisher);
            }

            assertTrue(latch.await(20, TimeUnit.SECONDS), "Publishers did not complete");
            disruptor.shutdown(5, TimeUnit.SECONDS);

            for (Publisher publisher : publishers)
            {
                assertThat(publisher.failed, is(false));
            }

            for (TestEventHandler handler : handlers)
            {
                assertThat(handler.messagesSeen, is(iterations * publisherCount));
                assertThat(handler.failureCount, is(0));
            }
        }
        finally
        {
            disruptor.halt();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS), "Publishers did not stop");
        }
    }

    private static Stream<WaitStrategy> waitStrategies()
    {
        return Stream.of(new BusySpinWaitStrategy(), new BlockingWaitStrategy());
    }

    private Publisher[] initialise(
            final Publisher[] publishers, final RingBuffer<TestEvent> buffer,
            final int messageCount, final CyclicBarrier barrier, final CountDownLatch latch)
    {
        for (int i = 0; i < publishers.length; i++)
        {
            publishers[i] = new Publisher(buffer, messageCount, barrier, latch);
        }

        return publishers;
    }

    @SuppressWarnings("unchecked")
    private TestEventHandler[] initialise(final Disruptor<TestEvent> disruptor, final TestEventHandler[] testEventHandlers)
    {
        for (int i = 0; i < testEventHandlers.length; i++)
        {
            TestEventHandler handler = new TestEventHandler();
            disruptor.handleEventsWith(handler);
            testEventHandlers[i] = handler;
        }

        return testEventHandlers;
    }

    private static class TestEventHandler implements EventHandler<TestEvent>
    {
        public int failureCount = 0;
        public int messagesSeen = 0;

        TestEventHandler()
        {
        }

        @Override
        public void onEvent(final TestEvent event, final long sequence, final boolean endOfBatch) throws Exception
        {
            if (event.sequence != sequence ||
                event.a != sequence + 13 ||
                event.b != sequence - 7 ||
                !("wibble-" + sequence).equals(event.s))
            {
                failureCount++;
            }

            messagesSeen++;
        }
    }

    private static class Publisher implements Runnable
    {
        private final RingBuffer<TestEvent> ringBuffer;
        private final CyclicBarrier barrier;
        private final int iterations;
        private final CountDownLatch shutdownLatch;

        public boolean failed = false;

        Publisher(
            final RingBuffer<TestEvent> ringBuffer,
            final int iterations,
            final CyclicBarrier barrier,
            final CountDownLatch shutdownLatch)
        {
            this.ringBuffer = ringBuffer;
            this.barrier = barrier;
            this.iterations = iterations;
            this.shutdownLatch = shutdownLatch;
        }

        @Override
        public void run()
        {
            try
            {
                barrier.await();

                int i = iterations;
                while (--i != -1)
                {
                    long next = ringBuffer.next();
                    TestEvent testEvent = ringBuffer.get(next);
                    testEvent.sequence = next;
                    testEvent.a = next + 13;
                    testEvent.b = next - 7;
                    testEvent.s = "wibble-" + next;
                    ringBuffer.publish(next);
                }
            }
            catch (Exception e)
            {
                failed = true;
            }
            finally
            {
                shutdownLatch.countDown();
            }
        }
    }

    private static class TestEvent
    {
        public long sequence;
        public long a;
        public long b;
        public String s;

        public static final EventFactory<TestEvent> FACTORY = () -> new TestEvent();
    }
}

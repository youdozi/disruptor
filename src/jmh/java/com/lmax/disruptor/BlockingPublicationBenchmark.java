package com.lmax.disruptor;

import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.Blackhole;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@BenchmarkMode({Mode.Throughput, Mode.SampleTime})
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(2)
@Threads(1)
@State(Scope.Benchmark)
public class BlockingPublicationBenchmark
{
    @Param({"BLOCKING", "LITE_BLOCKING", "BUSY_SPIN"})
    public String waitStrategy;

    @Param({"65536"})
    public int bufferSize;

    @Param({"0"})
    public int handlerWork;

    @Param({"0"})
    public long handlerPauseNanos;

    private Disruptor<Event> disruptor;
    private RingBuffer<Event> buffer;
    private Thread consumer;
    private long consumed;
    private long checksum;

    private static final class Event
    {
        long value;
    }

    @Setup
    public void setup(final BenchmarkParams params)
    {
        if (handlerWork < 0 || handlerPauseNanos < 0)
        {
            throw new IllegalArgumentException("Handler work and pause must be non-negative");
        }
        if (handlerPauseNanos > 0 && (bufferSize < 1 || handlerPauseNanos > TimeUnit.SECONDS.toNanos(5) / bufferSize))
        {
            throw new IllegalArgumentException("Configured pause would require more than five seconds to drain the ring");
        }
        final WaitStrategy strategy = switch (waitStrategy)
        {
            case "BLOCKING" -> new BlockingWaitStrategy();
            case "LITE_BLOCKING" -> new LiteBlockingWaitStrategy();
            case "BUSY_SPIN" -> new BusySpinWaitStrategy();
            default -> throw new IllegalArgumentException("Unknown wait strategy: " + waitStrategy);
        };
        disruptor = new Disruptor<>(Event::new, bufferSize, task ->
        {
            consumer = Thread.ofPlatform().daemon().unstarted(task);
            return consumer;
        }, params.getThreads() == 1 ? ProducerType.SINGLE : ProducerType.MULTI, strategy);
        disruptor.handleEventsWith((event, sequence, endOfBatch) ->
        {
            if (event.value != sequence)
            {
                throw new IllegalStateException("Event payload does not match its sequence");
            }
            long value = event.value;
            for (int i = 0; i < handlerWork; i++)
            {
                value = Long.rotateLeft(value ^ 0x9E3779B97F4A7C15L, 7) * 0xD1342543DE82EF95L;
            }
            checksum ^= value;
            if (handlerPauseNanos > 0)
            {
                Thread.sleep(Duration.ofNanos(handlerPauseNanos));
            }
            consumed++;
        });
        buffer = disruptor.start();
    }

    @Benchmark
    public void publish()
    {
        final long sequence = buffer.next();
        buffer.get(sequence).value = sequence;
        buffer.publish(sequence);
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void publishPaced(final Pacing pacing)
    {
        pacing.awaitBatch();
        publish();
    }

    @State(Scope.Thread)
    public static class Pacing
    {
        @Param({"1000"})
        public int eventsPerMillisecond;

        private int remaining;
        private long deadline;

        @Setup(Level.Iteration)
        public void reset()
        {
            if (eventsPerMillisecond < 1)
            {
                throw new IllegalArgumentException("eventsPerMillisecond must be positive");
            }
            deadline = 0;
        }

        private void awaitBatch()
        {
            if (deadline == 0)
            {
                deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1);
                remaining = eventsPerMillisecond;
            }
            if (remaining == 0)
            {
                long delay;
                while ((delay = deadline - System.nanoTime()) > 0)
                {
                    LockSupport.parkNanos(delay);
                }
                deadline += TimeUnit.MILLISECONDS.toNanos(1);
                remaining = eventsPerMillisecond;
            }
            remaining--;
        }
    }

    @TearDown
    public void tearDown(final Blackhole blackhole) throws Exception
    {
        disruptor.shutdown(10, TimeUnit.SECONDS);
        if (!consumer.join(Duration.ofSeconds(10)))
        {
            throw new IllegalStateException("Consumer did not stop");
        }
        if (consumed != buffer.getCursor() + 1)
        {
            throw new IllegalStateException("Published and consumed event counts differ");
        }
        blackhole.consume(checksum);
    }
}

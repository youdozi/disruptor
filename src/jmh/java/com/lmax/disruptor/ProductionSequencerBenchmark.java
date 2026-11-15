package com.lmax.disruptor;

import com.lmax.disruptor.dsl.ProducerType;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class ProductionSequencerBenchmark
{
    private static final int BATCH_SIZE = 32;

    @Param({"SINGLE", "MULTI"})
    public ProducerType producerType;

    @Param({"1024", "65536"})
    public int bufferSize;

    private Sequencer sequencer;
    private Sequence consumed;

    @Setup
    public void setup()
    {
        sequencer = producerType == ProducerType.SINGLE
                ? new SingleProducerSequencer(bufferSize, new BusySpinWaitStrategy())
                : new MultiProducerSequencer(bufferSize, new BusySpinWaitStrategy());
        consumed = new Sequence();
        sequencer.addGatingSequences(consumed);
    }

    @Benchmark
    public long publishSingle()
    {
        final long sequence = sequencer.next();
        sequencer.publish(sequence);
        consumed.set(sequence);
        return sequence;
    }

    @Benchmark
    @OperationsPerInvocation(BATCH_SIZE)
    public long publishBatch()
    {
        final long high = sequencer.next(BATCH_SIZE);
        sequencer.publish(high - BATCH_SIZE + 1, high);
        consumed.set(high);
        return high;
    }
}

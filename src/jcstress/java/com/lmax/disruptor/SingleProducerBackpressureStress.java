package com.lmax.disruptor;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Mode;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.Signal;
import org.openjdk.jcstress.annotations.State;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

@JCStressTest(Mode.Termination)
@Outcome(id = "TERMINATED", expect = ACCEPTABLE, desc = "The producer observes reclaimed capacity.")
@Outcome(id = "STALE", expect = FORBIDDEN, desc = "The producer did not observe consumer progress.")
@State
public class SingleProducerBackpressureStress
{
    private final Sequence consumed = new Sequence();
    private final Sequencer sequencer = new SingleProducerSequencer(16, new BusySpinWaitStrategy());

    public SingleProducerBackpressureStress()
    {
        sequencer.addGatingSequences(consumed);
    }

    @Actor
    public void awaitCapacity()
    {
        final long high = sequencer.next(16);
        sequencer.publish(0, high);
        if (sequencer.next() != 16)
        {
            throw new AssertionError("Unexpected sequence after reclaiming capacity");
        }
    }

    @Signal
    public void releaseCapacity()
    {
        consumed.set(0);
    }
}

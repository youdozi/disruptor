package com.lmax.disruptor;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Mode;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.Signal;
import org.openjdk.jcstress.annotations.State;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

public class BlockingWaitStrategyStress
{
    @JCStressTest(Mode.Termination)
    @Outcome(id = "TERMINATED", expect = ACCEPTABLE, desc = "Publication wakes the consumer and exposes its payload.")
    @Outcome(id = "STALE", expect = FORBIDDEN, desc = "Publication was missed.")
    @State
    public static class Publication
    {
        private final Sequencer sequencer = new SingleProducerSequencer(16, new BlockingWaitStrategy());
        private final SequenceBarrier barrier = sequencer.newBarrier();
        private int payload;

        @Actor
        public void awaitPublication() throws AlertException, InterruptedException, TimeoutException
        {
            barrier.waitFor(0);
            if (payload != 1)
            {
                throw new AssertionError("Published payload is not visible");
            }
        }

        @Signal
        public void publish()
        {
            payload = 1;
            sequencer.publish(0);
        }
    }

    @JCStressTest(Mode.Termination)
    @Outcome(id = "TERMINATED", expect = ACCEPTABLE, desc = "Alert releases a waiting consumer.")
    @Outcome(id = "STALE", expect = FORBIDDEN, desc = "Alert was missed.")
    @State
    public static class Alert
    {
        private final Sequencer sequencer = new SingleProducerSequencer(16, new BlockingWaitStrategy());
        private final SequenceBarrier barrier = sequencer.newBarrier();

        @Actor
        public void awaitAlert() throws InterruptedException, TimeoutException
        {
            try
            {
                barrier.waitFor(0);
            }
            catch (final AlertException expected)
            {
                return;
            }
            throw new AssertionError("Wait returned without an alert");
        }

        @Signal
        public void alert()
        {
            barrier.alert();
        }
    }
}

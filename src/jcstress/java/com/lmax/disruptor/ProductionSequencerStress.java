package com.lmax.disruptor;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.ZZ_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

public class ProductionSequencerStress
{
    @JCStressTest
    @Outcome(id = {"false, false", "false, true", "true, true"}, expect = ACCEPTABLE, desc = "Payload precedes publication.")
    @Outcome(expect = FORBIDDEN, desc = "Published slot observed without its payload.")
    @State
    public static class Publication
    {
        private final Sequencer sequencer = new MultiProducerSequencer(64, new BusySpinWaitStrategy());
        private boolean payload;

        @Actor
        public void writer()
        {
            final long sequence = sequencer.next();
            payload = true;
            sequencer.publish(sequence);
        }

        @Actor
        public void reader(final ZZ_Result result)
        {
            result.r1 = sequencer.isAvailable(0);
            result.r2 = payload;
        }
    }

    @JCStressTest
    @Outcome(id = {"false, false", "true, false", "true, true"}, expect = ACCEPTABLE, desc = "Publication order is preserved.")
    @Outcome(expect = FORBIDDEN, desc = "Later publication observed without earlier publication.")
    @State
    public static class OrderedPublication
    {
        private final Sequencer sequencer = new MultiProducerSequencer(64, new BusySpinWaitStrategy());

        @Actor
        public void writer()
        {
            final long high = sequencer.next(2);
            sequencer.publish(high - 1);
            sequencer.publish(high);
        }

        @Actor
        public void reader(final ZZ_Result result)
        {
            result.r2 = sequencer.isAvailable(1);
            result.r1 = sequencer.isAvailable(0);
        }
    }
}

package com.lmax.disruptor;

import org.openjdk.jcstress.annotations.Actor;
import org.openjdk.jcstress.annotations.Arbiter;
import org.openjdk.jcstress.annotations.JCStressTest;
import org.openjdk.jcstress.annotations.Outcome;
import org.openjdk.jcstress.annotations.State;
import org.openjdk.jcstress.infra.results.JJ_Result;
import org.openjdk.jcstress.infra.results.J_Result;

import static org.openjdk.jcstress.annotations.Expect.ACCEPTABLE;
import static org.openjdk.jcstress.annotations.Expect.FORBIDDEN;

public class ProductionSequenceStress
{
    @JCStressTest
    @Outcome(id = "2", expect = ACCEPTABLE, desc = "Both increments are retained.")
    @Outcome(expect = FORBIDDEN, desc = "An increment was lost.")
    @State
    public static class Increment
    {
        private final Sequence sequence = new Sequence(0);

        @Actor
        public void first()
        {
            sequence.incrementAndGet();
        }

        @Actor
        public void second()
        {
            sequence.incrementAndGet();
        }

        @Arbiter
        public void result(final J_Result result)
        {
            result.r1 = sequence.get();
        }
    }

    @JCStressTest
    @Outcome(id = {"0, 0", "0, 1", "1, 1"}, expect = ACCEPTABLE, desc = "Publication ordering is preserved.")
    @Outcome(expect = FORBIDDEN, desc = "Published sequence observed without its payload.")
    @State
    public static class Publication
    {
        private final Sequence sequence = new Sequence(0);
        private long payload;

        @Actor
        public void writer()
        {
            payload = 1;
            sequence.set(1);
        }

        @Actor
        public void reader(final JJ_Result result)
        {
            result.r1 = sequence.get();
            result.r2 = payload;
        }
    }

    @JCStressTest
    @Outcome(id = {"0, 1", "1, 0", "1, 1"}, expect = ACCEPTABLE, desc = "Volatile writes are ordered.")
    @Outcome(expect = FORBIDDEN, desc = "Both volatile writes were missed.")
    @State
    public static class VolatileOrdering
    {
        private final Sequence first = new Sequence(0);
        private final Sequence second = new Sequence(0);

        @Actor
        public void first(final JJ_Result result)
        {
            first.setVolatile(1);
            result.r1 = second.get();
        }

        @Actor
        public void second(final JJ_Result result)
        {
            second.setVolatile(1);
            result.r2 = first.get();
        }
    }
}

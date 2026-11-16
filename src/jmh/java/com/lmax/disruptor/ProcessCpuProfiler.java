package com.lmax.disruptor;

import com.sun.management.OperatingSystemMXBean;
import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.profile.InternalProfiler;
import org.openjdk.jmh.results.AggregationPolicy;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.Result;
import org.openjdk.jmh.results.ScalarResult;

import java.lang.management.ManagementFactory;
import java.util.Collection;
import java.util.List;

public class ProcessCpuProfiler implements InternalProfiler
{
    private final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    private long cpuStarted;
    private long startedAt;

    @Override
    public String getDescription()
    {
        return "Process CPU time as equivalent fully busy cores";
    }

    @Override
    public void beforeIteration(final BenchmarkParams benchmarkParams, final IterationParams iterationParams)
    {
        cpuStarted = os.getProcessCpuTime();
        startedAt = System.nanoTime();
    }

    @Override
    public Collection<? extends Result> afterIteration(
            final BenchmarkParams benchmarkParams, final IterationParams iterationParams, final IterationResult result)
    {
        final long elapsed = System.nanoTime() - startedAt;
        final long cpu = os.getProcessCpuTime() - cpuStarted;
        if (cpuStarted < 0 || cpu < 0)
        {
            throw new UnsupportedOperationException("Process CPU time is unavailable");
        }
        return List.of(new ScalarResult("process.cpu", (double) cpu / elapsed, "cores", AggregationPolicy.AVG));
    }
}

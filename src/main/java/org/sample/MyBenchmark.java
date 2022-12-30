package org.sample;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.util.concurrent.ExecutionException;

public class MyBenchmark {
    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public void testMethod(MyState state) throws ExecutionException, InterruptedException {
        state.sendStateToCluster();
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    public void testMethodMaster(MyState state) throws ExecutionException, InterruptedException {
        state.sendStateToClusterMaster();
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MyBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

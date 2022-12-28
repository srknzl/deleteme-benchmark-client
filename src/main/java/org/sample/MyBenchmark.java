package org.sample;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class MyBenchmark {
    @Benchmark
    @Fork(value = 1, warmups = 0)
    @Warmup(iterations = 1, time = 100, timeUnit = TimeUnit.MILLISECONDS)
    @Measurement(iterations = 1, time = 100, timeUnit = TimeUnit.MILLISECONDS)
    @BenchmarkMode(Mode.SingleShotTime)
    public void testMethod(MyState state) throws ExecutionException, InterruptedException {
        state.sendStateToCluster();
    }

//    @Benchmark
//    @Warmup(iterations = 0, time = 100, timeUnit = TimeUnit.MILLISECONDS)
//    @Measurement(iterations = 1, time = 100, timeUnit = TimeUnit.MILLISECONDS)
//    @BenchmarkMode(Mode.SingleShotTime)
//    public void testMethodMaster(MyState state) throws ExecutionException, InterruptedException {
//        state.sendStateToClusterMaster();
//    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(MyBenchmark.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }
}

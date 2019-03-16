package io.mycat.mycat2.bufferTest;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)//基准测试类型
@OutputTimeUnit(TimeUnit.SECONDS)//基准测试结果的时间类型
@Warmup(iterations = 10)//预热的迭代次数
@Threads(1)//测试线程数量
@State(Scope.Thread)//该状态为每个线程独享
//度量:iterations进行测试的轮次，time每轮进行的时长，timeUnit时长单位,batchSize批次数量
@Measurement(iterations = 10, time = -1, timeUnit = TimeUnit.SECONDS, batchSize = -1)
public class JMHLamdbaTest {
    static final int loopCount = 1 << 16;

    interface MutilArgsFun {
        int apply(int one, int two, int three, int four);
    }

    MutilArgsFun fun = (q, w, e, r) -> {
        int s = q + w + e + r;
        return Integer.bitCount(s);
    };

    int process(MutilArgsFun fun, int i) {
        return fun.apply(i, i, i, i);
    }

    @Setup
    public void init() {
    }

    @Benchmark
    public int outLoopLamdba() {
        int res =0;
        for (int i = 0; i < loopCount; i++) {
            res +=  process(fun, i);
        }
        return res;
    }

    @Benchmark
    public int inLoopLamdba() {
        int res =0;
        for (int i = 0; i < loopCount; i++) {
            res +=  process((q, w, e, r) -> {
                int s = q + w + e + r;
              return   Integer.bitCount(s);
            }, i);
        }
        return res;
    }

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(JMHLamdbaTest.class.getSimpleName())
                .forks(1)
//                .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+LogCompilation", "-XX:+TraceClassLoading", "-XX:+PrintAssembly")
                .addProfiler(GCProfiler.class)    // report GC time
                //.output("SQLBenchmark.log")//输出信息到文件
                .build();
        new Runner(opt).run();
    }
}

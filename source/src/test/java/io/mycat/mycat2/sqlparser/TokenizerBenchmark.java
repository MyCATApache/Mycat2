package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.SQLParseUtils.HashArray;
import io.mycat.mycat2.sqlparser.SQLParseUtils.Tokenizer;
//import io.mycat.mycat2.sqlparser.SQLParseUtils.UnsafeHashArray;
//import io.mycat.mycat2.sqlparser.SQLParseUtils.UnsafeTokenizer;
import org.openjdk.jmh.annotations.*;
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
public class TokenizerBenchmark {

    final byte[] srcBytes = {83, 69, 76, 69, 67, 84, 32, 97, 32, 70, 82, 79, 77, 32, 97, 98, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 32, 44, 32, 101, 101, 46, 102, 102, 32, 65, 83, 32, 102, 44, 40, 83, 69, 76, 69, 67, 84, 32, 97, 32, 70, 82, 79, 77, 32, 96, 115, 99, 104, 101, 109, 97, 95, 98, 98, 96, 46, 96, 116, 98, 108, 95, 98, 98, 96, 44, 40, 83, 69, 76, 69, 67, 84, 32, 97, 32, 70, 82, 79, 77, 32, 99, 99, 99, 32, 65, 83, 32, 99, 44, 32, 96, 100, 100, 100, 100, 96, 41, 41, 59};
    final int len = srcBytes.length;
    static final int MARK = 32;
    HashArray hashArray = new HashArray();
    Tokenizer tokenizer = new Tokenizer(hashArray);
//    UnsafeHashArray unsafeHashArray = new UnsafeHashArray();
//    UnsafeTokenizer unsafeTokenizer = new UnsafeTokenizer(unsafeHashArray);

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(TokenizerBenchmark.class.getSimpleName())
                .forks(1)
                .build();
        new Runner(opt).run();
    }

//    @Benchmark
//    public void normalTokenizer() {
//        tokenizer.tokenize(srcBytes);
//    }
//
//    @Benchmark
//    public void unsafeTokenizer() {
//        unsafeTokenizer.tokenize(srcBytes);
//    }

//    @Benchmark
    public void hashOp() {
        hashArray.init();
        int pos = 0;
        byte[] src = srcBytes;
        while (pos < len) {
            hashArray.set(src[pos], pos++, 1);
        }
    }

//    @Benchmark
//    public void hashOp2() {
//        unsafeHashArray.init();
//        int pos = 0;
//        byte[] src = srcBytes;
//        while (pos < len) {
//            unsafeHashArray.set(src[pos], pos++, 1);
//        }
//    }

//    @Benchmark
    public int innerVal2() {
        int pos = 0;
        int result = 0;
        byte[] src = srcBytes;
        while (pos < len) {
            if (src[pos++]==32)
                result++;
        }
        return result;
    }

    @Benchmark
    public void ParseSingleArray() {
//        byte[] a = new byte[8192];
        byte[] a = new byte[32768];
        int pos = 0;
        byte[] src = srcBytes;
        while (pos < len) {
            a[pos] = src[pos];
//            a[len+pos] = src[pos];
//            a[len*2+pos] = src[pos];
//            a[len*3+pos] = src[pos];
            pos++;
        }
    }

    @Benchmark
    public void ParseMultiArray() {
        byte[] a = new byte[8192];
        byte[] b = new byte[8192];
//        byte[] c = new byte[4096];
//        byte[] d = new byte[4096];
        int pos = 0;
        byte[] src = srcBytes;
        while (pos < len) {
            a[pos] = src[pos];
            b[pos] = src[pos];
//            c[pos] = src[pos];
//            d[pos] = src[pos];
            pos++;
        }

    }
}

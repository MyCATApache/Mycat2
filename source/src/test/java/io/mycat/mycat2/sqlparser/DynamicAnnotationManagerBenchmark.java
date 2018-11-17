//package io.mycat.mycat2.sqlparser;
//
//import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.*;
//import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.DynamicAnnotationKeyRoute;
//import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.impl.SQLType;
//import junit.framework.TestCase;
//import org.openjdk.jmh.annotations.*;
//import org.openjdk.jmh.profile.GCProfiler;
//import org.openjdk.jmh.runner.Runner;
//import org.openjdk.jmh.runner.RunnerException;
//import org.openjdk.jmh.runner.options.Options;
//import org.openjdk.jmh.runner.options.OptionsBuilder;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.concurrent.TimeUnit;
//
///**
// * Created by jamie on 2017/9/15.
// */
//@BenchmarkMode(Mode.Throughput)//基准测试类型
//@OutputTimeUnit(TimeUnit.SECONDS)//基准测试结果的时间类型
//@Warmup(iterations = 10)//预热的迭代次数
//@Threads(1)//测试线程数量
//@State(Scope.Thread)//该状态为每个线程独享
////度量:iterations进行测试的轮次，time每轮进行的时长，timeUnit时长单位,batchSize批次数量
//@Measurement(iterations = 5, time = -1, timeUnit = TimeUnit.SECONDS, batchSize = -1)
////@CompilerControl() //http://javadox.com/org.openjdk.jmh/jmh-core/0.9/org/openjdk/jmh/annotations/CompilerControl.Mode.html
//public class DynamicAnnotationManagerBenchmark extends TestCase {
//
//
//    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicAnnotationManagerBenchmark.class);
//
//
//    public static void main(String[] args) throws RunnerException {
//        Options opt = new OptionsBuilder()
//                .include(DynamicAnnotationManagerBenchmark.class.getSimpleName())
//                .forks(1)
////                .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+LogCompilation", "-XX:+TraceClassLoading", "-XX:+PrintAssembly")
//                .addProfiler(GCProfiler.class)    // report GC time
//                //.output("SQLBenchmark.log")//输出信息到文件
//                .build();
//        new Runner(opt).run();
//
//    }
//    @Benchmark
//    public void stateOneAnd() throws Exception{
//        sqlParser.parse(bytes,context);
//    }
//    @Benchmark
//    public void stateOneAndDynamicAnnotation() throws Exception{
//        sqlParser.parse(bytes,context);
//       manager.process(schemaHash,sqlType,intsTables,context);
//    }
//    @Benchmark
//    public void dynamicAnnotation() throws Exception{
//        manager.process(schemaHash,sqlType,intsTables,context);
//    }
//    DynamicAnnotationManagerImpl manager;
//    BufferSQLContext context;
//    BufferSQLParser sqlParser;
//    String[] tables=new String[]{"x1"};
//    int[] intsTables= DynamicAnnotationKeyRoute.stringArray2HashArray(tables);
//    int schemaHash="schemA".hashCode();
//    int sqlType= SQLType.INSERT.ordinal();
//    byte[] bytes="b = 1 and c = 1 and d = a.b and c = 1".getBytes();
//    @Setup
//    public void init() throws Exception {
//        manager=new DynamicAnnotationManagerImpl("actions_bak.yml", "annotations_bak.yml");
//        context=new BufferSQLContext();
//        sqlParser=new BufferSQLParser();
//        sqlParser.parse(bytes,context);
//    }
//
//
//}
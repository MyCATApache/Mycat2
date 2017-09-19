package io.mycat.mycat2.sqlparser;

import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.*;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Match;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Matches;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.RootBean;
import io.mycat.mycat2.sqlparser.byteArrayInterface.dynamicAnnotation.pojo.Schema;
import io.mycat.util.YamlUtil;
import junit.framework.TestCase;
import org.junit.Before;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Created by jamie on 2017/9/15.
 */
@BenchmarkMode(Mode.Throughput)//基准测试类型
@OutputTimeUnit(TimeUnit.SECONDS)//基准测试结果的时间类型
@Warmup(iterations = 5)//预热的迭代次数
@Threads(1)//测试线程数量
@State(Scope.Thread)//该状态为每个线程独享
//度量:iterations进行测试的轮次，time每轮进行的时长，timeUnit时长单位,batchSize批次数量
@Measurement(iterations = 5, time = -1, timeUnit = TimeUnit.SECONDS, batchSize = -1)
//@CompilerControl() //http://javadox.com/org.openjdk.jmh/jmh-core/0.9/org/openjdk/jmh/annotations/CompilerControl.Mode.html
public class DynamicAnnotationManagerTest extends TestCase {


    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicAnnotationManagerTest.class);

    //动态注解先匹配chema的名字,再sql类型，在匹配表名，在匹配条件
    public static void main(String[] args) throws Exception {
        DynamicAnnotationManager manager=new DynamicAnnotationManager("actions.yaml","annotations.yaml");
        BufferSQLContext context=new BufferSQLContext();
        BufferSQLParser sqlParser=new BufferSQLParser();
        String str="select * where id between 1 and 100 and name = \"haha\" and a=1 and name2 = \"ha\"";
        System.out.println(str);
        sqlParser.parse(str.getBytes(),context);
        manager.prototype(new HashMap<>()).process("schemA",SQLType.INSERT,new String[]{"x1"},context).run();
    }
    //run
//    public static void main(String[] args) throws RunnerException {
//        Options opt = new OptionsBuilder()
//                .include(DynamicAnnotationManagerTest.class.getSimpleName())
//                .forks(1)
////                .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+LogCompilation", "-XX:+TraceClassLoading", "-XX:+PrintAssembly")
//                .addProfiler(GCProfiler.class)    // report GC time
//                //.output("SQLBenchmark.log")//输出信息到文件
//                .build();
//        new Runner(opt).run();
//
//    }
// static    RouteMap<Runnable> routeMap;
//    static {
//        Map<int[], Runnable> map = new HashMap<>();
//
//        for (int i = 0; i < (100000 - 2); i++) {
//            int[] array = new int[]{i, i + 1, i + 2,i+3};
//            map.put(array, () -> System.out.println(Arrays.toString(array)));
//        }
//        long times = System.currentTimeMillis();
//        System.out.println("=>" + times);
//        routeMap = new RouteMap<>(map, Runnable.class);
//    }
//
//    //空循环 对照项
//  static  int[] s=  new int[]{999,1,2,5};
//    @Benchmark
//    public void emptyLoop() {
//            routeMap.get(s);
//    }
//    @Before
//    protected void setUp() throws Exception {
//
//
//    }
}
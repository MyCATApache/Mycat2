package io.mycat.mycat2.sqlparser;


//import com.alibaba.druid.sql.SQLUtils;
//import com.alibaba.druid.sql.ast.SQLStatement;
//import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;

import io.mycat.mycat2.sqlparser.byteArrayInterface.ByteArrayInterface;
import io.mycat.mycat2.sqlparser.byteArrayInterface.DefaultByteArray;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@BenchmarkMode(Mode.Throughput)//基准测试类型
@OutputTimeUnit(TimeUnit.SECONDS)//基准测试结果的时间类型
@Warmup(iterations = 20)//预热的迭代次数
@Threads(1)//测试线程数量
@State(Scope.Thread)//该状态为每个线程独享
//度量:iterations进行测试的轮次，time每轮进行的时长，timeUnit时长单位,batchSize批次数量
@Measurement(iterations = 10, time = -1, timeUnit = TimeUnit.SECONDS, batchSize = -1)
public class SQLBenchmark {
    SQLParser parser;
    SQLContext context;
    NewSQLParser newSQLParser;
    NewSQLContext newSQLContext;
    BufferSQLParser newSQLParser2;
    BufferSQLContext newSQLContext2;
    byte[] srcBytes;
    String src;
    ByteArrayInterface byteArrayInterface;

    //run
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(SQLBenchmark.class.getSimpleName())
                .forks(1)
                .jvmArgs("-XX:+UnlockDiagnosticVMOptions", "-XX:+LogCompilation", "-XX:+TraceClassLoading", "-XX:+PrintAssembly")
                .addProfiler(GCProfiler.class)    // report GC time
                //.output("SQLBenchmark.log")//输出信息到文件
                .build();
        new Runner(opt).run();
    }

    @Setup
    public void init() {
        src = "SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));";
        srcBytes = src.getBytes(StandardCharsets.UTF_8);//20794
        byteArrayInterface=new DefaultByteArray(srcBytes);
        parser = new SQLParser();
        context = new SQLContext();
        newSQLParser = new NewSQLParser();
        newSQLContext = new NewSQLContext();
        newSQLParser2 = new BufferSQLParser();
        newSQLContext2 = new BufferSQLContext();
        //newSQLParser.init();
//        unsafeSQLParser = new NewUnsafeSQLParser();
//        unsafeSQLParser.init();
        System.out.println("=> init");
    }

    @Benchmark
    public void NewSqQLParserTest() {
        newSQLParser.parse(srcBytes, newSQLContext);
    }

    @Benchmark
    public void NewSqQLParserWithByteArrayInterfaceTest() {
        newSQLParser2.parse(byteArrayInterface, newSQLContext2);
    }

//    @Benchmark
//    public void UnsafeSqQLParserTest() { unsafeSQLParser.tokenize(srcBytes);}

//    @Benchmark
    public void SQLParserTest() {
        parser.parse(srcBytes, context);
    }

//    @Benchmark
//    public void DruidTest() {
//        List<SQLStatement> stmtList = SQLUtils.parseStatements(src, "mysql");
//    }

//    public void DruidParse() {
//        List<SQLStatement> stmtList = SQLUtils.parseStatements(src, "mysql");
//        //解析出的独立语句的个数
//        System.out.println("size is:" + stmtList.size());
//        for (int i = 0; i < stmtList.size(); i++) {
//            SQLStatement stmt = stmtList.get(i);
//            MySqlSchemaStatVisitor visitor = new MySqlSchemaStatVisitor();
//            stmt.accept(visitor);
//            //获取表名称
//            System.out.println("Tables : " + visitor.getCurrentTable());
//            //获取操作方法名称,依赖于表名称
//            System.out.println("Manipulation : " + visitor.getTables());
//            //获取字段名称
//            System.out.println("fields : " + visitor.getColumns());
//        }
//    }
}
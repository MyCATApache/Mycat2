//package cn.lightfish.pattern.benchmarks;
//
//import com.alibaba.fastsql.DbType;
//import com.alibaba.fastsql.sql.parser.SQLParserUtils;
//import com.alibaba.fastsql.sql.parser.SQLStatementParser;
//import org.openjdk.jmh.annotations.*;
//
//import java.nio.ByteBuffer;
//import java.nio.charset.StandardCharsets;
//
//public class FastSQLBenchMark2 {
//    private final static byte[]         bytes = "select id from travelrecord where id = 1;".getBytes();;
//
//    @State(Scope.Benchmark)
//    public static class ExecutionPlan {
//
//
//        public int iterations;
//
//        public static final ByteBuffer password = StandardCharsets.UTF_8.encode("select id from travelrecord where id = jjjj;");
////        public static final
////
////        static {
////        }
//
//        @Setup(Level.Invocation)
//        public void setUp() {
//
//        }
//    }
//
//    @Fork(value = 1, warmups = 1)
//    @Benchmark
//    @BenchmarkMode(Mode.Throughput)
//    @Warmup(iterations = 1)
//    public void benchMurmur3_128(ExecutionPlan plan) {
//        SQLStatementParser sqlStatementParser;
//
//        sqlStatementParser = SQLParserUtils.createSQLStatementParser(new String(bytes),DbType.mysql);
//    sqlStatementParser.parseSelect();
//    }
//}
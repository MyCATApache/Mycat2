package io.mycat.sqlparser.util.simpleParser2;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import io.mycat.sqlparser.util.simpleParser.BufferSQLContext;
import io.mycat.sqlparser.util.simpleParser.BufferSQLParser;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class BufferSQLParserBenchMark2 {
    private final static byte[]         bytes = "SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));".getBytes();;

    static  final   BufferSQLParser parser = new BufferSQLParser();
    static final  BufferSQLContext context = new BufferSQLContext();
    @State(Scope.Benchmark)
    public static class ExecutionPlan {

        public int iterations;

        public static final ByteBuffer password = StandardCharsets.UTF_8.encode("select id from travelrecord where id = jjjj;");
//        public static final
//
//        static {
//        }

        @Setup(Level.Invocation)
        public void setUp() {

        }
    }

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 1)
    public void benchMurmur3_128(ExecutionPlan plan) {
        parser.parse(bytes, context);
    }
}
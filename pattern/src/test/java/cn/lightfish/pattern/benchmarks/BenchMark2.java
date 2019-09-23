package cn.lightfish.pattern.benchmarks;

import cn.lightfish.pattern.*;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashMap;

/**
 * https://github.com/junwen12221/GPattern.git
 *
 * @author Junwen Chen
 **/
public class BenchMark2 {
    public static final ByteBuffer password = StandardCharsets.UTF_8.encode("SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));");

    @Fork(value = 1, warmups = 1)
    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    @Warmup(iterations = 1)
    public void benchMurmur3_128(ExecutionPlan plan) throws CloneNotSupportedException {
        GPattern pattern = ExecutionPlan.pattern;
        GPatternUTF8Lexer utf8Lexer = pattern.getUtf8Lexer();
        GPatternIdRecorder idRecorder = pattern.getIdRecorder();
        GPatternMatcher matcher = pattern.getMatcher();
//        utf8Lexer.init(password, 0, password.limit());
//        matcher.reset();
//        while (utf8Lexer.nextToken()){
//    idRecorder.toCurToken();
//        });
        GPatternMatcher matcher1 = pattern.matcherAndCollect(password);

        if (!matcher1.acceptAll()) {
            System.out.println("-------------------------------------");
        }
    }

    public static void main(String[] args) {
//        GPattern pattern = ExecutionPlan.pattern;
//        while (true) {
//            GPatternUTF8Lexer utf8Lexer = pattern.getUtf8Lexer();
//            GPatternIdRecorder idRecorder = pattern.getIdRecorder();
//
//            while (utf8Lexer.nextToken()){
//                GPatternToken gPatternToken = idRecorder.toCurToken();
//            }
//        }

        GPattern pattern = ExecutionPlan.pattern;
        GPatternUTF8Lexer utf8Lexer = pattern.getUtf8Lexer();
        GPatternIdRecorder idRecorder = pattern.getIdRecorder();
        GPatternMatcher matcher = pattern.getMatcher();
//        utf8Lexer.init(password, 0, password.limit());
//        matcher.reset();
//        while (utf8Lexer.nextToken()){
//         idRecorder.toCurToken();
//        }
        while (true) {
            GPatternMatcher matcher1 = pattern.matcher(password);
            if (!matcher1.acceptAll()) {
                System.out.println("-------------------------------------");
            }
        }
    }

    @State(Scope.Benchmark)
    public static class ExecutionPlan {


        public int iterations;

        private static final GPattern pattern;

        static {
            GPatternBuilder patternBuilder = new GPatternBuilder(0);

            HashMap<String, Collection<String>> res = new HashMap<>();
            GPatternCollectorTest.addTable(res, "ee", "ff");
            GPatternCollectorTest.addTable(res, "schema_bb", "tbl_bb");
            GPatternCollectorTest.addTable(res, "db1", "ccc");
            GPatternCollectorTest.addTable(res, "db1", "dddd");
            //  int i = builder.addRule("SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));");
            int i2 = patternBuilder.addRule("SELECT a FROM ab             , ee.ff AS f,(SELECT a FROM `schema_bb`.`tbl_bb`,(SELECT a FROM ccc AS c, `dddd`));");
            TableCollectorBuilder builder = new TableCollectorBuilder(patternBuilder.geIdRecorder(), res);
            TableCollector tableCollector = builder.create();
            pattern = patternBuilder.createGroupPattern(tableCollector);
        }

        @Setup(Level.Invocation)
        public void setUp() {

        }
    }

}
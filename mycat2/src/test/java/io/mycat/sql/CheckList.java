package io.mycat.sql;

import io.mycat.dao.TestUtil;
import lombok.SneakyThrows;
import org.junit.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;

public class CheckList {

    @Test
    public void testAggSQLChecker() throws Exception {
        test(statement -> new AggSQLChecker(statement).run()) ;
    }

    @Test
    public void testCharSQLChecker() throws Exception {
        test(statement -> new CharChecker(statement).run()) ;
    }

    @Test
    public void testDateSQLChecker() throws Exception {
        test(statement -> new DateChecker(statement).run()) ;
    }
    @Test
    public void testHBTChecker() throws Exception {
        test(statement -> new HBTChecker(statement).run()) ;
    }
    @Test
    public void testHBTMappingChecker() throws Exception {
        test(statement -> new HBTMappingChecker(statement).run()) ;
    }
    @Test
    public void testJoinSQLChecker() throws Exception {
        test(statement -> new JoinSQLChecker(statement).run()) ;
    }
    @Test
    public void testMathChecker() throws Exception {
        test(statement -> new MathChecker(statement).run()) ;
    }
    @Test
    public void testSQLExprChecker() throws Exception {
        test(statement -> new SQLExprChecker(statement).run()) ;
    }
    @SneakyThrows
    private static void test(Consumer<Statement> consumer){
        List<String> initList = Arrays.asList("set xa = off");
        try (Connection mySQLConnection = TestUtil.getMySQLConnection()) {
            try(Statement statement = mySQLConnection.createStatement()){
                for (String u : initList) {
                    statement.execute(u);
                }
                consumer.accept(statement);
            }
        }
    }
}
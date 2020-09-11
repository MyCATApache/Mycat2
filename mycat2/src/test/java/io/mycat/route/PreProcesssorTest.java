//package io.mycat.route;
//
//import com.alibaba.fastsql.sql.SQLUtils;
//import com.alibaba.fastsql.sql.ast.SQLStatement;
//import org.junit.Assert;
//import org.junit.Test;
//
//public class PreProcesssorTest {
//
//    @Test
//    public void test() {
//        String defaultSchema = "db1";
//        String sql = "select id  from travelrecord";
//        String s = process(defaultSchema, sql);
//        Assert.assertEquals("SELECT travelrecord.id AS id\n" +
//                "FROM db1.travelrecord travelrecord", s);
//    }
//
//    @Test
//    public void test1() {
//        String defaultSchema = "db1";
//        String sql = "select t.id  from travelrecord as t join db2.company as c on t.id = c.id";
//        String s = process(defaultSchema, sql);
//        Assert.assertEquals("SELECT t.id AS `t.id`\n" +
//                "FROM db1.travelrecord t\n" +
//                "\tJOIN db2.company c ON t.id = c.id", s);
//    }
//
//    @Test
//    public void test2() {
//        String defaultSchema = "db1";
//        String sql = "select t.id  from travelrecord as t join db2.company as c on t.id = c.id";
//        String s = process(defaultSchema, sql);
//        Assert.assertEquals("SELECT t.id AS `t.id`\n" +
//                "FROM db1.travelrecord t\n" +
//                "\tJOIN db2.company c ON t.id = c.id", s);
//    }
//
//    private String process(String defaultSchema, String sql) {
//        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
//        Scope scope = new Scope(defaultSchema);
//
//        sqlStatement.accept(scope);
//        scope.build();
//        PreProcesssor preProcesssor = new PreProcesssor(defaultSchema);
//        sqlStatement.accept(preProcesssor);
//        return sqlStatement.toString();
//    }
//}
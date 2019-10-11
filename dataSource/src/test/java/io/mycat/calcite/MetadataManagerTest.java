package io.mycat.calcite;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.parser.SQLParserUtils;
import com.alibaba.fastsql.sql.parser.SQLStatementParser;
import io.mycat.calcite.shardingQuery.SchemaInfo;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class MetadataManagerTest {
    static Map<BackendTableInfo, String> routeDelete(String currentSchema, String sql) {
        return MetadataManager.INSATNCE.rewriteUpdateSQL(currentSchema, sql);
    }

    public List<BackendTableInfo> getBackEndTableInfo(String schemaName, String tableName, String startValue, String endValue) {
        return MetadataManager.INSATNCE.getBackEndTableInfo(schemaName, tableName, startValue, endValue);
    }

    public BackendTableInfo getBackEndTableInfo(String schemaName, String tableName, String partitionValue) {
        return MetadataManager.INSATNCE.getBackEndTableInfo(schemaName, tableName, partitionValue);
    }

    static Iterator<Map<BackendTableInfo, String>> routeInsert(String currentSchema, String sql) {
        SQLStatementParser sqlStatementParser = SQLParserUtils.createSQLStatementParser(sql, DbType.mysql);
        List list = new LinkedList();
        sqlStatementParser.parseStatementList(list);
        return MetadataManager.INSATNCE.getInsertInfoIterator(currentSchema, (Iterator<MySqlInsertStatement>) list.iterator());
    }

    @Test
    public void test() {
        Map<BackendTableInfo, String> rs = routeDelete("TESTDB", "DELETE FROM travelrecord WHERE id = 1");
        Map.Entry<BackendTableInfo, String> next = rs.entrySet().iterator().next();
        String sql = next.getValue();
        Assert.assertTrue(sql.contains("db1.travelrecord"));
    }

    @Test
    public void test1() {
        Map<BackendTableInfo, String> rs = routeDelete("TESTDB", "DELETE FROM travelrecord WHERE user_id = '2' ");
        System.out.println(rs);
        Assert.assertTrue(rs.containsValue("DELETE FROM db2.travelrecord3\n" +
                "WHERE user_id = '2'"));
        Assert.assertTrue(rs.containsValue("DELETE FROM db2.travelrecord2\n" +
                "WHERE user_id = '2'"));
        assertEquals(9, rs.size());
    }

    @Test
    public void test2() {
        Map<BackendTableInfo, String> rs = routeDelete("TESTDB", "DELETE FROM travelrecord");
        System.out.println(rs);
        assertEquals(9, rs.size());
    }

    @Test
    public void test3() {
        Iterator<Map<BackendTableInfo, String>> iterator = routeInsert("TESTDB", "INSERT INTO `travelrecord` (`id`) VALUES ('4'); ");
        Map<BackendTableInfo, String> next = iterator.next();
        Assert.assertTrue(next.containsValue("INSERT INTO db1.travelrecord (`id`)\n" +
                "VALUES ('4');"));
    }

    @Test
    public void test4() {
        Iterator<Map<BackendTableInfo, String>> iterator = routeInsert("TESTDB", "INSERT INTO `travelrecord` (`id`) VALUES ('4'); ");
        Map<BackendTableInfo, String> next = iterator.next();
        Assert.assertTrue(next.containsValue("INSERT INTO db1.travelrecord (`id`)\n" +
                "VALUES ('4');"));
    }

    @Test
    public void test5() {
        Iterator<Map<BackendTableInfo, String>> iterator = routeInsert("TESTDB", "INSERT INTO `travelrecord` (`id`) VALUES ('4'),('999'); ");
        Map<BackendTableInfo, String> next = iterator.next();
        assertEquals(2, next.size());
    }

    @Test
    public void test6() {
        Iterator<Map<BackendTableInfo, String>> iterator = routeInsert("TESTDB", "INSERT INTO `travelrecord` (`id`) VALUES ('4'),('999'); INSERT INTO `travelrecord` (`id`) VALUES ('2000');");
        Map<BackendTableInfo, String> next = iterator.next();
        assertEquals(2, next.size());
        Map<BackendTableInfo, String> next2 = iterator.next();
        assertEquals(1, next2.size());
    }

    @Test
    public void test7() {
        BackendTableInfo backEndTableInfo = getBackEndTableInfo("TESTDB", "travelrecord", "1");
        SchemaInfo schemaInfo = backEndTableInfo.getSchemaInfo();
        String targetSchemaTable = schemaInfo.getTargetSchemaTable();
        Assert.assertEquals("db1.travelrecord", targetSchemaTable);
    }

    @Test
    public void test8() {
        List<BackendTableInfo> backEndTableInfo = getBackEndTableInfo("TESTDB", "travelrecord", "1",String.valueOf(Integer.MAX_VALUE));
        Assert.assertEquals(3, backEndTableInfo.size());
    }
}
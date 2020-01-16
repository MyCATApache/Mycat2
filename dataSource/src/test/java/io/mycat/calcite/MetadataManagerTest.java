package io.mycat.calcite;

import io.mycat.BackendTableInfo;
import org.junit.Assert;
import org.junit.Test;

import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static io.mycat.calcite.MetadataManager.routeInsert;
import static io.mycat.calcite.MetadataManager.routeInsertFlat;
import static org.junit.Assert.assertEquals;

public class MetadataManagerTest {
    public MetadataManagerTest() {
        MetadataManager instance = MetadataManager.INSTANCE;
        MetadataManagerBuilder.exampleBuild(instance);
    }

    static Map<String, List<String>> routeDelete(String currentSchema, String sql) {
        return MetadataManager.INSTANCE.rewriteSQL(currentSchema, sql);
    }

    public List<BackendTableInfo> getBackEndTableInfo(String schemaName, String tableName, String startValue, String endValue) {
        return MetadataManager.INSTANCE.getNatrueBackEndTableInfo(schemaName, tableName, startValue, endValue);
    }

    public BackendTableInfo getBackEndTableInfo(String schemaName, String tableName, String partitionValue) {
        return MetadataManager.INSTANCE.getNatrueBackEndTableInfo(schemaName, tableName, partitionValue);
    }


    @Test
    public void test() {
        Map<String, List<String>> rs = routeDelete("db1", "DELETE FROM travelrecord WHERE id = 1");
        Map.Entry<String, List<String>> next = rs.entrySet().iterator().next();
        List<String> sql = next.getValue();
        Assert.assertTrue(sql.get(0).contains("db1.travelrecord"));
    }

    @Test
    public void test1() {
        Map<String, List<String>> rs = routeDelete("db1", "DELETE FROM travelrecord WHERE user_id = '2' ");
        List<String> collect = getStrings(rs);
        Assert.assertTrue(collect.contains("DELETE FROM db2.travelrecord3\n" +
                "WHERE user_id = '2'"));
        Assert.assertTrue(collect.contains("DELETE FROM db2.travelrecord2\n" +
                "WHERE user_id = '2'"));
        assertEquals(9, collect.size());
    }

    @Test
    public void test2() {
        Map<String, List<String>> rs = routeDelete("db1", "DELETE FROM travelrecord");
        List<String> collect = getStrings(rs);
        assertEquals(9, collect.size());
    }

    private List<String> getStrings(Map<String, List<String>> rs) {
        return rs.values().stream().flatMap(i -> i.stream()).collect(Collectors.toList());
    }

    @Test
    public void test3() {
        List<String> strings = getStrings(routeInsertFlat("db1", "INSERT INTO `travelrecord` (`id`) VALUES ('4'); "));
        Assert.assertTrue(strings.contains("INSERT INTO db1.travelrecord (`id`)\n" +
                "VALUES ('4');"));
    }

    @Test
    public void test4() {
        List<String> strings = getStrings(routeInsertFlat("db1", "INSERT INTO `travelrecord` (`id`) VALUES ('4'); "));
        Assert.assertTrue(strings.contains("INSERT INTO db1.travelrecord (`id`)\n" +
                "VALUES ('4');"));
    }

    @Test
    public void test5() {
        List<String> strings = getStrings(routeInsertFlat("db1", "INSERT INTO `travelrecord` (`id`) VALUES ('4'),('999'); "));
        assertEquals(2, strings.size());
    }

    @Test
    public void test6() {
        Iterable<Map<String, List<String>>> iterable = routeInsert("db1", "INSERT INTO `travelrecord` (`id`) VALUES ('4'),('999'); INSERT INTO `travelrecord` (`id`) VALUES ('2000');");
        Iterator<Map<String, List<String>>> iterator = iterable.iterator();
        Map<String, List<String>> next = iterator.next();
        Map<String, List<String>> next2 = iterator.next();
        assertEquals("{defaultDatasourceName=[INSERT INTO db1.travelrecord3 (`id`)\n" +
                "VALUES ('999');, INSERT INTO db1.travelrecord (`id`)\n" +
                "VALUES ('4');]}", next.toString());
        assertEquals("{defaultDatasourceName=[INSERT INTO db1.travelrecord3 (`id`)\n" +
                "VALUES ('2000');]}", next2.toString());
    }

    @Test
    public void test7() {
        String sql = "DELETE FROM travelrecord WHERE id = '2' ";
        String id = "2";
        BackendTableInfo backEndTableInfo = getBackEndTableInfo("db1", "travelrecord", id);
        String newSQL = MessageFormat.format("DELETE FROM {0} WHERE user_id = {1} ", backEndTableInfo.getSchemaInfo().getTargetSchemaTable(), id);
    }

    @Test
    public void test8() {
        List<BackendTableInfo> backEndTableInfo = getBackEndTableInfo("db1", "travelrecord", "1", String.valueOf(Integer.MAX_VALUE));
        Assert.assertEquals(9, backEndTableInfo.size());
    }
}
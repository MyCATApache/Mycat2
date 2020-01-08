package io.mycat.calcite;

import io.mycat.BackendTableInfo;
import org.junit.Assert;

import java.text.MessageFormat;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.mycat.calcite.MetadataManager.routeInsert;
import static org.junit.Assert.assertEquals;

public class MetadataManagerTest {
    public MetadataManagerTest() {
        MetadataManager instance = MetadataManager.INSTANCE;
        instance.addSchema("");
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



   // @Test
    public void test() {
        Map<String, List<String>> rs = routeDelete("TESTDB", "DELETE FROM travelrecord WHERE id = 1");
        Map.Entry<String,List< String>> next = rs.entrySet().iterator().next();
        List< String> sql = next.getValue();
        Assert.assertTrue(sql.contains("db1.travelrecord"));
    }

   // @Test
    public void test1() {
        Map<String, List<String>> rs = routeDelete("TESTDB", "DELETE FROM travelrecord WHERE user_id = '2' ");
        System.out.println(rs);
        Assert.assertTrue(rs.containsValue("DELETE FROM db2.travelrecord3\n" +
                "WHERE user_id = '2'"));
        Assert.assertTrue(rs.containsValue("DELETE FROM db2.travelrecord2\n" +
                "WHERE user_id = '2'"));
        assertEquals(9, rs.size());
    }

   // @Test
    public void test2() {
        Map<String,List<String>> rs = routeDelete("TESTDB", "DELETE FROM travelrecord");
        System.out.println(rs);
        assertEquals(9, rs.size());
    }

   // @Test
    public void test3() {
        Iterator<Map<String, String>> iterator = routeInsert("TESTDB", "INSERT INTO `travelrecord` (`id`) VALUES ('4'); ");
        Map<String, String> next = iterator.next();
        Assert.assertTrue(next.containsValue("INSERT INTO db1.travelrecord (`id`)\n" +
                "VALUES ('4');"));
    }

   // @Test
    public void test4() {
        Iterator<Map<String, String>> iterator = routeInsert("TESTDB", "INSERT INTO `travelrecord` (`id`) VALUES ('4'); ");
        Map<String, String> next = iterator.next();
        Assert.assertTrue(next.containsValue("INSERT INTO db1.travelrecord (`id`)\n" +
                "VALUES ('4');"));
    }

   // @Test
    public void test5() {
        Iterator<Map<String, String>> iterator = routeInsert("TESTDB", "INSERT INTO `travelrecord` (`id`) VALUES ('4'),('999'); ");
        Map<String, String> next = iterator.next();
        assertEquals(2, next.size());
    }

   // @Test
    public void test6() {
        Iterator<Map<String, String>> iterator = routeInsert("TESTDB", "INSERT INTO `travelrecord` (`id`) VALUES ('4'),('999'); INSERT INTO `travelrecord` (`id`) VALUES ('2000');");
        Map<String, String> next = iterator.next();
        assertEquals(2, next.size());
        Map<String, String> next2 = iterator.next();
        assertEquals(1, next2.size());
    }

   // @Test
    public void test7() {
        String sql = "DELETE FROM travelrecord WHERE id = '2' ";
        String id = "2";
        BackendTableInfo backEndTableInfo = getBackEndTableInfo("TESTDB", "travelrecord", id);
        String newSQL = MessageFormat.format("DELETE FROM {0} WHERE user_id = {1} ", backEndTableInfo.getSchemaInfo().getTargetSchemaTable(), id);
    }

   // @Test
    public void test8() {
        List<BackendTableInfo> backEndTableInfo = getBackEndTableInfo("TESTDB", "travelrecord", "1",String.valueOf(Integer.MAX_VALUE));
        Assert.assertEquals(3, backEndTableInfo.size());
    }
}
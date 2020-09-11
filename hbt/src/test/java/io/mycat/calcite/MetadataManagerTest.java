//package io.mycat.calcite;
//
//import io.mycat.DataNode;
//import io.mycat.metadata.MetadataManager;
//import io.mycat.metadata.MetadataManagerBuilder;
//import org.junit.Assert;
//import org.junit.Test;
//
//import java.text.MessageFormat;
//import java.util.Iterator;
//import java.util.List;
//import java.util.Map;
//import java.util.stream.Collectors;
//
//import static io.mycat.metadata.MetadataManager.routeInsert;
//import static io.mycat.metadata.MetadataManager.routeInsertFlat;
//import static org.junit.Assert.assertEquals;
//
//public class MetadataManagerTest {
//    public MetadataManagerTest() {
//        MetadataManager instance = MetadataManager.INSTANCE;
//        MetadataManagerBuilder.exampleBuild(instance);
//    }
//
//    static Map<String, List<String>> routeDelete(String currentSchema, String sql) {
//        return MetadataManager.INSTANCE.rewriteSQL(currentSchema, sql);
//    }
//
////    public List<DataNode> getBackEndTableInfo(String schemaName, String tableName, String startValue, String endValue) {
////        return MetadataManager.INSTANCE.getNatrueBackEndTableInfo(schemaName, tableName, startValue, endValue);
////    }
//
////    public DataNode getBackEndTableInfo(String schemaName, String tableName, String partitionValue) {
////        return MetadataManager.INSTANCE.getNatrueBackEndTableInfo(schemaName, tableName, partitionValue);
////    }
//
//
//    @Test
//    public void test() {
//        Map<String, List<String>> rs = routeDelete("db1", "DELETE FROM travelrecord WHERE id = 1");
//        Map.Entry<String, List<String>> next = rs.entrySet().iterator().next();
//        List<String> sql = next.getValue();
//        String s = sql.get(0).toLowerCase();
//        Assert.assertTrue(s.contains("db1.travelrecord"));
//    }
//
//    /**
//     * 测试无区分大小写
//     */
//    @Test
//    public void test1() {
//        Map<String, List<String>> rs = routeDelete("db1", "DELETE FROM travelrecord WHERE user_id = '2' ");
//        List<String> collect = getStrings(rs);
//        Assert.assertTrue(collect.contains("DELETE FROM db2.travelrecord3\nWHERE user_id = '2'".toLowerCase()));
//        Assert.assertTrue(collect.contains("DELETE FROM db2.travelrecord2\nWHERE user_id = '2'".toLowerCase()));
//        assertEquals(9, collect.size());
//    }
//
//    @Test
//    public void test2() {
//        Map<String, List<String>> rs = routeDelete("db1", "DELETE FROM travelrecord");
//        List<String> collect = getStrings(rs);
//        assertEquals(9, collect.size());
//    }
//
//    private List<String> getStrings(Map<String, List<String>> rs) {
//        return rs.values().stream().flatMap(i -> i.stream()).map(i->i.toLowerCase()).collect(Collectors.toList());
//    }
//
//    @Test
//    public void test3() {
//        List<String> strings = getStrings(routeInsertFlat("db1", "INSERT INTO `travelrecord` (`id`) VALUES ('4'); "));
//        Assert.assertTrue(strings.contains("INSERT INTO db1.travelrecord (`id`)\nVALUES ('4');".toLowerCase()));
//    }
//
//    @Test
//    public void test4() {
//        List<String> strings = getStrings(routeInsertFlat("db1", "INSERT INTO `travelrecord` (`id`) VALUES ('4'); "));
//        Assert.assertTrue(strings.contains("INSERT INTO db1.travelrecord (`id`)\nVALUES ('4');".toLowerCase()));
//    }
//
//    @Test
//    public void test5() {
//        List<String> strings = getStrings(routeInsertFlat("db1", "INSERT INTO `travelrecord` (`id`) VALUES ('4'),('999'); "));
//        assertEquals(2, strings.size());
//    }
//
//    @Test
//    public void test6() {
//        Iterable<Map<String, List<String>>> iterable = routeInsert("db1", "INSERT INTO `travelrecord` (`id`) VALUES ('4'),('999'); INSERT INTO `travelrecord` (`id`) VALUES ('2000');");
//        Iterator<Map<String, List<String>>> iterator = iterable.iterator();
//        Map<String, List<String>> next = iterator.next();
//        Map<String, List<String>> next2 = iterator.next();
//        assertEquals(("{defaultdatasourcename=[insert into db1.travelrecord (`id`)\n" +
//                "values ('4');, insert into db1.travelrecord3 (`id`)\n" +
//                "values ('999');]}").toLowerCase(), next.toString().toLowerCase());
//        assertEquals("{defaultDatasourceName=[INSERT INTO db1.travelrecord3 (`id`)\nVALUES ('2000');]}".toLowerCase(), next2.toString().toLowerCase());
//    }
//
////    @Test
////    public void test7() {
////        String sql = "DELETE FROM travelrecord WHERE id = '2' ";
////        String id = "2";
////        DataNode backEndTableInfo = getBackEndTableInfo("db1", "travelrecord", id);
////        String newSQL = MessageFormat.format("DELETE FROM {0} WHERE user_id = {1} ", backEndTableInfo.getTargetSchemaTable(), id);
////    }
////
////    @Test
////    public void test8() {
////        List<DataNode> backEndTableInfo = getBackEndTableInfo("db1", "travelrecord", "1", String.valueOf(Integer.MAX_VALUE));
////        Assert.assertEquals(9, backEndTableInfo.size());
////    }
//}
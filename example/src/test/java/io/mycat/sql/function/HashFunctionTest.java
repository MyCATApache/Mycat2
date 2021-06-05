package io.mycat.sql.function;

import com.google.common.collect.ImmutableMap;
import io.mycat.DataNode;
import io.mycat.MetadataManager;
import io.mycat.RangeVariable;
import io.mycat.RangeVariableType;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.config.ShardingFuntion;
import io.mycat.config.ShardingTableConfig;
import io.mycat.router.CustomRuleFunction;
import io.mycat.util.JsonUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class HashFunctionTest extends AutoFunctionFactoryTest{

    /**
     * https://help.aliyun.com/document_detail/71276.html
     * <p>
     * 若分库和分表都使用同一个拆分键进行HASH时，则根据拆分键的键值按总的分表数取余。
     * 例如有2个分库，每个分库4张分表，那么0库上保存分表0~3，1库上保存分表4~7。某个键值为15，那么根据该路由方式，则该键值15将被分到1库的表7上（（15 % (2 * 4) =7））
     */
    @Test
    public void testHashIdHashId() {
        ShardingTableConfig mainSharding = new ShardingTableConfig();
        mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 4;");
        mainSharding.setFunction(ShardingFuntion.builder().properties(JsonUtil.from("{\n" +
                "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                "\t\t\t\t\t\"tableNum\":\"4\",\n" +
                "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                "\t\t\t\t\t\"storeNum\":1,\n" +
                "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                "\t\t\t\t}", Map.class)).build());
        MetadataManager metadataManager = getMetadataManager(mainSharding);
        ShardingTable tableHandler = (ShardingTable) metadataManager.getTable("db1", "sharding");
        CustomRuleFunction shardingFuntion = tableHandler.getShardingFuntion();
        List<DataNode> calculate = shardingFuntion.calculate(Collections.singletonMap("id", Collections.singleton(new RangeVariable("id", RangeVariableType.EQUAL, 15))));
        String s = calculate.toString();
        Assert.assertTrue(s.contains("[{targetName='c0', schemaName='db1_1', tableName='sharding_3', dbIndex=1, tableIndex=7}]"));

        String s1 = shardingFuntion.calculate(Collections.emptyMap()).toString();
        Assert.assertEquals("[{targetName='c0', schemaName='db1_0', tableName='sharding_0', dbIndex=0, tableIndex=0}, {targetName='c0', schemaName='db1_0', tableName='sharding_1', dbIndex=0, tableIndex=1}, {targetName='c0', schemaName='db1_0', tableName='sharding_2', dbIndex=0, tableIndex=2}, {targetName='c0', schemaName='db1_0', tableName='sharding_3', dbIndex=0, tableIndex=3}, {targetName='c0', schemaName='db1_1', tableName='sharding_0', dbIndex=1, tableIndex=4}, {targetName='c0', schemaName='db1_1', tableName='sharding_1', dbIndex=1, tableIndex=5}, {targetName='c0', schemaName='db1_1', tableName='sharding_2', dbIndex=1, tableIndex=6}, {targetName='c0', schemaName='db1_1', tableName='sharding_3', dbIndex=1, tableIndex=7}]",
                s1);
        System.out.println();
    }

    @Test
    public void testHashDbId() {
        ShardingTableConfig mainSharding = new ShardingTableConfig();
        mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                + " dbpartition by hash(id);");
        mainSharding.setFunction(ShardingFuntion.builder().properties(JsonUtil.from("{\n" +
                "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                "\t\t\t\t\t\"storeNum\":1,\n" +
                "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                "\t\t\t\t}", Map.class)).build());
        MetadataManager metadataManager = getMetadataManager(mainSharding);
        ShardingTable tableHandler = (ShardingTable) metadataManager.getTable("db1", "sharding");
        CustomRuleFunction shardingFuntion = tableHandler.getShardingFuntion();
        String s1 = shardingFuntion.calculate(Collections.emptyMap()).toString();
        Assert.assertEquals("[{targetName='c0', schemaName='db1_0', tableName='sharding_0', dbIndex=0, tableIndex=0}, {targetName='c0', schemaName='db1_1', tableName='sharding_0', dbIndex=1, tableIndex=1}]",
                s1);
        System.out.println();
    }

    @Test
    public void testHashTableId() {
        ShardingTableConfig mainSharding = new ShardingTableConfig();
        mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                + " tbpartition by hash(id) tbpartitions 2;");
        mainSharding.setFunction(ShardingFuntion.builder().properties(JsonUtil.from("{\n" +
                "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                "\t\t\t\t\t\"tableNum\":\"2\",\n" +
                "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
                "\t\t\t\t\t\"storeNum\":1,\n" +
                "\t\t\t\t}", Map.class)).build());
        MetadataManager metadataManager = getMetadataManager(mainSharding);
        ShardingTable tableHandler = (ShardingTable) metadataManager.getTable("db1", "sharding");
        CustomRuleFunction shardingFuntion = tableHandler.getShardingFuntion();
        List<DataNode> calculate = shardingFuntion.calculate(Collections.singletonMap("id", Collections.singleton(new RangeVariable("id", RangeVariableType.EQUAL, 15))));
        Assert.assertEquals(1, calculate.size());
        Assert.assertEquals(2, shardingFuntion.calculate(Collections.emptyMap()).size());
        System.out.println();
    }

    @Test
    public void testHashIdHashUserId() {
        ShardingTableConfig mainSharding = new ShardingTableConfig();
        mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
                "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
                "  `user_id` varchar(100) DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int DEFAULT NULL,\n" +
                "  `blob` longblob,\n" +
                "  PRIMARY KEY (`id`),\n" +
                "  KEY `id` (`id`)\n" +
                ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
                + " dbpartition by hash(id) tbpartition by hash(user_id) tbpartitions 2 dbpartitions 4;");
        mainSharding.setFunction(ShardingFuntion.builder().properties(JsonUtil.from("{\n" +
                "\t\t\t\t\t\"dbNum\":\"2\",\n" +
                "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
                "\t\t\t\t\t\"tableNum\":\"4\",\n" +
                "\t\t\t\t\t\"tableMethod\":\"hash(user_id)\",\n" +
                "\t\t\t\t\t\"storeNum\":1,\n" +
                "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
                "\t\t\t\t}", Map.class)).build());
        MetadataManager metadataManager = getMetadataManager(mainSharding);
        ShardingTable tableHandler = (ShardingTable) metadataManager.getTable("db1", "sharding");
        CustomRuleFunction shardingFuntion = tableHandler.getShardingFuntion();
        ImmutableMap<String, Collection<RangeVariable>> map = ImmutableMap.of("id", Collections.singletonList(new RangeVariable("id", RangeVariableType.EQUAL, 15)),
                "user_id", Collections.singletonList(new RangeVariable("user_id", RangeVariableType.EQUAL, 2))
        );
        List<DataNode> insertRoute = shardingFuntion.calculate(
                map);
        Assert.assertEquals(1, insertRoute.size());

        List<DataNode> firstColumn = shardingFuntion.calculate(
                ImmutableMap.of("id", Collections.singletonList(new RangeVariable("id", RangeVariableType.EQUAL, 15))));

        Assert.assertTrue(firstColumn.containsAll(insertRoute));

        List<DataNode> secondColumn = shardingFuntion.calculate(
                ImmutableMap.of("user_id", Collections.singletonList(new RangeVariable("user_id", RangeVariableType.EQUAL, 2))));

        Assert.assertTrue(firstColumn.containsAll(secondColumn));

        Assert.assertTrue(insertRoute.equals(secondColumn));

        String s1 = shardingFuntion.calculate(Collections.emptyMap()).toString();
        Assert.assertEquals("[{targetName='c0', schemaName='db1_0', tableName='sharding_0', dbIndex=0, tableIndex=0}, {targetName='c0', schemaName='db1_0', tableName='sharding_1', dbIndex=0, tableIndex=1}, {targetName='c0', schemaName='db1_0', tableName='sharding_2', dbIndex=0, tableIndex=2}, {targetName='c0', schemaName='db1_0', tableName='sharding_3', dbIndex=0, tableIndex=3}, {targetName='c0', schemaName='db1_1', tableName='sharding_0', dbIndex=1, tableIndex=4}, {targetName='c0', schemaName='db1_1', tableName='sharding_1', dbIndex=1, tableIndex=5}, {targetName='c0', schemaName='db1_1', tableName='sharding_2', dbIndex=1, tableIndex=6}, {targetName='c0', schemaName='db1_1', tableName='sharding_3', dbIndex=1, tableIndex=7}]",
                s1);

        System.out.println();
    }

}

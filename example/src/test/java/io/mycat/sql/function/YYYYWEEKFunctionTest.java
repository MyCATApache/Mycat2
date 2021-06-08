package io.mycat.sql.function;

import io.mycat.Partition;
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

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class YYYYWEEKFunctionTest extends AutoFunctionFactoryTest{

    /**
     https://help.aliyun.com/document_detail/71334.html
     */
    @Test
    public void testYYYYWEEK() {
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
                + " dbpartition by YYYYWEEK(traveldate) tbpartition by YYYYWEEK(traveldate) tbpartitions 3 dbpartitions 8;");
        mainSharding.setFunction(ShardingFuntion.builder().properties(JsonUtil.from("{\n" +
                "\t\t\t\t\t\"dbNum\":\"8\",\n" +
                "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/sharding_${index}\",\n" +
                "\t\t\t\t\t\"tableNum\":\"14\",\n" +
                "\t\t\t\t\t\"tableMethod\":\"YYYYWEEK(traveldate)\",\n" +
                "\t\t\t\t\t\"storeNum\":1,\n" +
                "\t\t\t\t\t\"dbMethod\":\"YYYYWEEK(traveldate)\"\n" +
                "\t\t\t\t}", Map.class)).build());
        MetadataManager metadataManager = getMetadataManager(mainSharding);
        ShardingTable tableHandler = (ShardingTable) metadataManager.getTable("db1", "sharding");
        CustomRuleFunction shardingFuntion = tableHandler.getShardingFuntion();
        List<Partition> calculate = shardingFuntion
                .calculate(Collections.singletonMap("traveldate",
                        Collections.singleton(new RangeVariable("traveldate", RangeVariableType.EQUAL,
                                LocalDate.of(2020,6,5)))));
        String s = calculate.toString();
        Assert.assertTrue(s.contains("[{targetName='c0', schemaName='db1_1', tableName='sharding_15', index=15, dbIndex=1, tableIndex=1}]"));

        Assert.assertEquals( 112,
                shardingFuntion.calculate(Collections.emptyMap()).size());
        System.out.println();
    }
}

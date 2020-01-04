package io.mycat.calcite;

import io.mycat.config.ShardingQueryRootConfig;
import io.mycat.config.SharingFuntionRootConfig;
import io.mycat.util.YamlUtil;

import java.util.*;

public class MetadataManagerBuilder {

    public static void exampleBuild(MetadataManager m) {
        m.addSchema("db1");
        ShardingQueryRootConfig.BackEndTableInfoConfig.BackEndTableInfoConfigBuilder builder = backEndBuilder();
        List<ShardingQueryRootConfig.BackEndTableInfoConfig> tableInfos = Arrays.asList(
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db1").tableName("TRAVELRECORD").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db1").tableName("TRAVELRECORD2").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db1").tableName("TRAVELRECORD3").build(),

                backEndBuilder().targetName("defaultDatasourceName").schemaName("db2").tableName("TRAVELRECORD").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db2").tableName("TRAVELRECORD2").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db2").tableName("TRAVELRECORD3").build(),

                backEndBuilder().targetName("defaultDatasourceName").schemaName("db3").tableName("TRAVELRECORD").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db3").tableName("TRAVELRECORD2").build(),
                backEndBuilder().targetName("defaultDatasourceName").schemaName("db3").tableName("TRAVELRECORD3").build()
        );

        Map<String, String> properties = new HashMap<>();
        properties.put("partitionCount", "2,1");
        properties.put("partitionLength", "256,512");

        ShardingQueryRootConfig.LogicTableConfig build = ShardingQueryRootConfig.LogicTableConfig.builder()
                .columns(Arrays.asList(ShardingQueryRootConfig.Column.builder()
                        .columnName("id").function(SharingFuntionRootConfig.ShardingFuntion.builder().name("partitionByLong")
                                .clazz("io.mycat.router.function.PartitionByLong").properties(properties).ranges(Collections.emptyMap())
                                .build()).shardingType(SimpleColumnInfo.ShardingType.NATURE_DATABASE_TABLE.name()).build()))
                .createTableSQL("CREATE TABLE `travelrecord` (\n" +
                        "  `id` bigint(20) NOT NULL,\n" +
                        "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                        "  `traveldate` date DEFAULT NULL,\n" +
                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                        "  `days` int(11) DEFAULT NULL,\n" +
                        "  `blob` longblob DEFAULT NULL,\n" +
                        "  `d` double DEFAULT NULL\n" +
                        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;")
                .build();
        System.out.println(YamlUtil.dump(build));
        m.addTable("db1", "travelrecord",build ,tableInfos,null);
        m.setDefaultTransactionType("jdbc");
    }

    public static ShardingQueryRootConfig.BackEndTableInfoConfig.BackEndTableInfoConfigBuilder backEndBuilder() {
        return ShardingQueryRootConfig.BackEndTableInfoConfig.builder();
    }
}
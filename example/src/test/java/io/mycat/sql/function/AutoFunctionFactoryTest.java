package io.mycat.sql.function;

import io.mycat.*;
import io.mycat.assemble.MycatTest;
import io.mycat.config.*;
import io.mycat.hint.CreateClusterHint;
import io.mycat.hint.CreateDataSourceHint;
import lombok.SneakyThrows;

import java.util.*;

public class AutoFunctionFactoryTest implements MycatTest {


    @SneakyThrows
    public static MetadataManager getMetadataManager(ShardingTableConfig shardingTableConfig) {
        System.setProperty("mode", "local");
        MycatCore mycatCore = new MycatCore();
        MetadataStorageManager fileMetadataStorageManager = MetaClusterCurrent.wrapper(MetadataStorageManager.class);
        MycatRouterConfig mycatRouterConfig = new MycatRouterConfig();
        LogicSchemaConfig logicSchemaConfig = new LogicSchemaConfig();
        mycatRouterConfig.getSchemas().add(logicSchemaConfig);
        logicSchemaConfig.setSchemaName("db1");

//                ShardingTableConfig mainSharding = new ShardingTableConfig();
//                mainSharding.setCreateTableSQL("CREATE TABLE db1.`sharding` (\n" +
//                        "  `id` bigint NOT NULL AUTO_INCREMENT,\n" +
//                        "  `user_id` varchar(100) DEFAULT NULL,\n" +
//                        "  `traveldate` date DEFAULT NULL,\n" +
//                        "  `fee` decimal(10,0) DEFAULT NULL,\n" +
//                        "  `days` int DEFAULT NULL,\n" +
//                        "  `blob` longblob,\n" +
//                        "  PRIMARY KEY (`id`),\n" +
//                        "  KEY `id` (`id`)\n" +
//                        ") ENGINE=InnoDB  DEFAULT CHARSET=utf8"
//                        + " dbpartition by hash(id) tbpartition by hash(id) tbpartitions 2 dbpartitions 2;");
//                mainSharding.setFunction(ShardingFuntion.builder().properties(JsonUtil.from("{\n" +
//                        "\t\t\t\t\t\"dbNum\":\"2\",\n" +
//                        "\t\t\t\t\t\"mappingFormat\":\"c${targetIndex}/db1_${dbIndex}/sharding_${tableIndex}\",\n" +
//                        "\t\t\t\t\t\"tableNum\":\"2\",\n" +
//                        "\t\t\t\t\t\"tableMethod\":\"hash(id)\",\n" +
//                        "\t\t\t\t\t\"storeNum\":2,\n" +
//                        "\t\t\t\t\t\"dbMethod\":\"hash(id)\"\n" +
//                        "\t\t\t\t}", Map.class)).build());
        logicSchemaConfig.getShardingTables().put("sharding", shardingTableConfig);

        mycatRouterConfig.getClusters().add(CreateClusterHint.createConfig("c0", Arrays.asList("ds0"), Collections.emptyList()));
        mycatRouterConfig.getClusters().add(CreateClusterHint.createConfig("c1", Arrays.asList("ds1"), Collections.emptyList()));

        mycatRouterConfig.getDatasources().add(CreateDataSourceHint.createConfig("ds0", DB1));
        mycatRouterConfig.getDatasources().add(CreateDataSourceHint.createConfig("ds1", DB2));
        mycatRouterConfig.getDatasources().add(CreateDataSourceHint.createConfig("prototype", DB1));
        fileMetadataStorageManager.start(mycatRouterConfig);
        return MetaClusterCurrent.wrapper(MetadataManager.class);
    }
}
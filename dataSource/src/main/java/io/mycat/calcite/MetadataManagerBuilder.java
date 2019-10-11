package io.mycat.calcite;

import io.mycat.calcite.shardingQuery.SchemaInfo;
import io.mycat.config.YamlUtil;
import io.mycat.config.shardingQuery.ShardingQueryRootConfig;

import java.util.*;

public class MetadataManagerBuilder {

    public static void exampleBuild(MetadataManager m) {
        m.addSchema("TESTDB");
        List<BackEndTableInfo> tableInfos = Arrays.asList(
                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db1").targetTable("TRAVELRECORD").build()).build(),
                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db1").targetTable("TRAVELRECORD2").build()).build(),
                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db1").targetTable("TRAVELRECORD3").build()).build(),

                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db2").targetTable("TRAVELRECORD").build()).build(),
                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db2").targetTable("TRAVELRECORD2").build()).build(),
                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db2").targetTable("TRAVELRECORD3").build()).build(),

                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db3").targetTable("TRAVELRECORD").build()).build(),
                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db3").targetTable("TRAVELRECORD2").build()).build(),
                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db3").targetTable("TRAVELRECORD3").build()).build()
        );
        m.addTable("TESTDB", "TRAVELRECORD", tableInfos);
        Map<String, String> properties = new HashMap<>();
        properties.put("partitionCount", "2,1");
        properties.put("partitionLength", "256,512");
        m.addTableDataMapping("TESTDB", "TRAVELRECORD", Arrays.asList("ID"), "partitionByLong", properties, Collections.emptyMap());
        m.addCreateTableSQL("TESTDB", "TRAVELRECORD", "CREATE TABLE `travelrecord` (\n" +
                "  `id` bigint(20) NOT NULL,\n" +
                "  `user_id` varchar(100) CHARACTER SET utf8 DEFAULT NULL,\n" +
                "  `traveldate` date DEFAULT NULL,\n" +
                "  `fee` decimal(10,0) DEFAULT NULL,\n" +
                "  `days` int(11) DEFAULT NULL,\n" +
                "  `blob` longblob DEFAULT NULL,\n" +
                "  `d` double DEFAULT NULL\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;");
        m.addCreateTableSQL("TESTDB", "ADDRESS", "create table `address` (" +
                "`id` int (11)," +
                "`addressname` varchar (80)" +
                ");");

        List<BackEndTableInfo> tableInfos2 = Arrays.asList(
                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db1").targetTable("address").build()).build(),
                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db2").targetTable("address").build()).build(),
                BackEndTableInfo.builder().hostName("mytest3306a").schemaInfo(SchemaInfo.builder().targetSchema("db3").targetTable("address").build()).build()
        );

        m.addTable("TESTDB", "ADDRESS", tableInfos2);
        properties.put("partitionCount", "2,1");
        properties.put("partitionLength", "256,512");
        m.addTableDataMapping("TESTDB", "ADDRESS", Arrays.asList("ID"), "partitionByLong", properties, Collections.emptyMap());

        ShardingQueryRootConfig rootConfig = new ShardingQueryRootConfig();
        List<ShardingQueryRootConfig.LogicSchemaConfig> metaMap = rootConfig.getSchemas();
        m.schemaBackendMetaMap.forEach((schemaName, tableList) -> {
            List<ShardingQueryRootConfig.LogicTableConfig> list = new ArrayList<>();
            metaMap.add(new ShardingQueryRootConfig.LogicSchemaConfig(schemaName, list));
            for (Map.Entry<String, List<BackEndTableInfo>> entry : tableList.entrySet()) {
                String tableName = entry.getKey().toLowerCase();
                List<ShardingQueryRootConfig.BackEndTableInfoConfig> backEndTableInfoConfigList = new ArrayList<>();
                List<BackEndTableInfo> endTableInfos = entry.getValue();
                for (BackEndTableInfo b : endTableInfos) {
                    backEndTableInfoConfigList.add(new ShardingQueryRootConfig.BackEndTableInfoConfig(
                            b.getDataNodeName(), b.getReplicaName(), b.getHostName(), b.getSchemaInfo().getTargetSchema(), b.getSchemaInfo().getTargetTable()));
                }
                DataMappingConfig dataMappingConfig = m.schemaDataMappingMetaMap.get(schemaName).get(tableName);
                Map<String, String> map = m.logicTableCreateSQLMap.get(schemaName);
                String sql = null;
                if (map!=null){
                    sql = map.get(tableName);
                }
                ShardingQueryRootConfig.LogicTableConfig logicTableConfig = new ShardingQueryRootConfig.LogicTableConfig(tableName, backEndTableInfoConfigList, dataMappingConfig.columnName,
                        dataMappingConfig.ruleAlgorithm.name(), dataMappingConfig.ruleAlgorithm.getProt(), dataMappingConfig.ruleAlgorithm.getRanges(), sql);
                list.add(logicTableConfig);
            }
        });
        String dump = YamlUtil.dump(rootConfig);
        System.out.println(dump);
    }
}
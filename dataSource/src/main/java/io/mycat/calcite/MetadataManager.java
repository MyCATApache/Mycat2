/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite;

import io.mycat.ConfigRuntime;
import io.mycat.config.ConfigFile;
import io.mycat.config.YamlUtil;
import io.mycat.config.shardingQuery.ShardingQueryRootConfig;
import io.mycat.router.RuleAlgorithm;
import io.mycat.router.function.PartitionRuleAlgorithmManager;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Junwen Chen
 **/
public enum MetadataManager {
    INSATNCE;
    private final static Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class);
    final Map<String, Map<String, List<BackEndTableInfo>>> schemaBackendMetaMap = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Map<String, List<SimpleColumnInfo>>> schemaColumnMetaMap = new ConcurrentHashMap<>();
    final ConcurrentHashMap<String, Map<String, DataMappingConfig>> schemaDataMappingMetaMap = new ConcurrentHashMap<>();

    MetadataManager() {
        final String charset = "UTF-8";
        System.setProperty("saffron.default.charset", charset);
        System.setProperty("saffron.default.nationalcharset", charset);
        System.setProperty("saffron.default.collat​​ion.name", charset + "$ en_US");
        PartitionRuleAlgorithmManager.INSTANCE.initFunctions(ConfigRuntime.INSTCANE.load().getConfig(ConfigFile.FUNCTIONS));
        ShardingQueryRootConfig shardingQueryRootConfig = (ShardingQueryRootConfig) ConfigRuntime.INSTCANE.getConfig(ConfigFile.SHARDING_QUERY);
        if (shardingQueryRootConfig == null) {
            addSchema("TESTDB");
            List<BackEndTableInfo> tableInfos = Arrays.asList(
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db1").tableName("TRAVELRECORD").build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db1").tableName("TRAVELRECORD2").build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db1").tableName("TRAVELRECORD3").build(),

                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db2").tableName("TRAVELRECORD").build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db2").tableName("TRAVELRECORD2").build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db2").tableName("TRAVELRECORD3").build(),

                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db3").tableName("TRAVELRECORD").build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db3").tableName("TRAVELRECORD2").build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db3").tableName("TRAVELRECORD3").build()
            );
            addTable("TESTDB", "TRAVELRECORD", tableInfos);
            Map<String, String> properties = new HashMap<>();
            properties.put("partitionCount", "2,1");
            properties.put("partitionLength", "256,512");
            addTableDataMapping("TESTDB", "TRAVELRECORD", Arrays.asList("ID"), "partitionByLong", properties, Collections.emptyMap());


            List<BackEndTableInfo> tableInfos2 = Arrays.asList(
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db1").tableName("address").build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db2").tableName("address").build(),
                    BackEndTableInfo.builder().hostName("mytest3306a").schemaName("db3").tableName("address").build()
            );

            addTable("TESTDB", "ADDRESS", tableInfos2);
            properties.put("partitionCount", "2,1");
            properties.put("partitionLength", "256,512");
            addTableDataMapping("TESTDB", "ADDRESS", Arrays.asList("ID"), "partitionByLong", properties, Collections.emptyMap());

            ShardingQueryRootConfig rootConfig = new ShardingQueryRootConfig();
            List<ShardingQueryRootConfig.LogicSchemaConfig> metaMap = rootConfig.getSchemas();
            schemaBackendMetaMap.forEach((schemaName, tableList) -> {
                List<ShardingQueryRootConfig.LogicTableConfig> list =  new ArrayList<>();
                metaMap.add(new ShardingQueryRootConfig.LogicSchemaConfig(schemaName,list));
                for (Map.Entry<String, List<BackEndTableInfo>> entry : tableList.entrySet()) {
                    String tableName = entry.getKey();
                    List<ShardingQueryRootConfig.BackEndTableInfoConfig> backEndTableInfoConfigList = new ArrayList<>();
                    List<BackEndTableInfo> endTableInfos = entry.getValue();
                    for (BackEndTableInfo b : endTableInfos) {
                        backEndTableInfoConfigList.add(new ShardingQueryRootConfig.BackEndTableInfoConfig(
                                b.getDataNodeName(), b.getReplicaName(), b.getHostName(), b.getSchemaName(), b.getTableName()));
                    }
                    DataMappingConfig dataMappingConfig = this.schemaDataMappingMetaMap.get(schemaName).get(tableName);
                    ShardingQueryRootConfig.LogicTableConfig logicTableConfig = new ShardingQueryRootConfig.LogicTableConfig(tableName, backEndTableInfoConfigList, dataMappingConfig.columnName,
                            dataMappingConfig.ruleAlgorithm.name(), dataMappingConfig.ruleAlgorithm.getProt(), dataMappingConfig.ruleAlgorithm.getRanges()
                    );
                    list.add(logicTableConfig);
                }
            });

            String dump = YamlUtil.dump(rootConfig);
            System.out.println(dump);
        } else {
            for (ShardingQueryRootConfig.LogicSchemaConfig entry : shardingQueryRootConfig.getSchemas()) {
                String schemaName = entry.getSchemaName();
                addSchema(schemaName);
                for (ShardingQueryRootConfig.LogicTableConfig tableConfigEntry : entry.getTables()) {
                    String tableName = tableConfigEntry.getTableName();
                    ArrayList<BackEndTableInfo> list = new ArrayList<>();
                    for (ShardingQueryRootConfig.BackEndTableInfoConfig b : tableConfigEntry.getQueryPhysicalTable()) {
                        list.add(new BackEndTableInfo(b.getDataNodeName(), b.getReplicaName(), b.getHostName(), b.getSchemaName(), b.getTableName()));
                    }
                    addTable(schemaName, tableName, list);
                    List<String> columns = tableConfigEntry.getColumns();
                    String function = tableConfigEntry.getFunction();
                    Map<String, String> properties = tableConfigEntry.getProperties();
                    Map<String, String> ranges = tableConfigEntry.getRanges();
                    addTableDataMapping(schemaName, tableName, columns, function, properties, ranges);
                }
            }

        }


        if (schemaColumnMetaMap.isEmpty()) {
            schemaColumnMetaMap.putAll(CalciteConvertors.columnInfoList(schemaBackendMetaMap));
        }
    }

    private <T, K, V> void addTableDataMapping(String schemaName, String tableName, List<String> columnList, String rule, Map<String, String> properties, Map<String, String> ranges) {
        schemaDataMappingMetaMap.compute(schemaName, (s, stringDataMappingEvaluatorMap) -> {
            if (stringDataMappingEvaluatorMap == null) {
                stringDataMappingEvaluatorMap = new HashMap<>();
            }
            RuleAlgorithm ruleAlgorithm = PartitionRuleAlgorithmManager.INSTANCE.getRuleAlgorithm(rule, properties, ranges);
            stringDataMappingEvaluatorMap.put(tableName, new DataMappingConfig(columnList, ruleAlgorithm));
            return stringDataMappingEvaluatorMap;
        });
    }

    private void addTableDataMapping(String schemaName, String tableName, List<String> columnList, String rule) {
        schemaDataMappingMetaMap.compute(schemaName, (s, stringDataMappingEvaluatorMap) -> {
            if (stringDataMappingEvaluatorMap == null) {
                stringDataMappingEvaluatorMap = new HashMap<>();
            }
            stringDataMappingEvaluatorMap.put(tableName, new DataMappingConfig(columnList, null));
            return stringDataMappingEvaluatorMap;
        });
    }


    public CalciteConnection getConnection() {
        try {
            Connection connection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false;lex=MYSQL;fun=mysql;conformance=MYSQL_5");
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            schemaBackendMetaMap.forEach((schemaName, tables) -> {
                SchemaPlus currentSchema = rootSchema.add(schemaName, new AbstractSchema());
                tables.forEach((tableName, value) -> {
                    List<SimpleColumnInfo> columnInfos = schemaColumnMetaMap.get(schemaName).get(tableName);
                    RowSignature rowSignature = CalciteConvertors.rowSignature(columnInfos);
                    Optional<DataMappingConfig> optional = Optional.ofNullable(schemaDataMappingMetaMap.get(schemaName)).flatMap(s -> Optional.ofNullable(s.get(tableName)));
                    DataMappingEvaluator dataMappingEvaluator = null;
                    if (optional.isPresent()) {
                        DataMappingConfig dataMappingConfig = optional.get();
                        RuleAlgorithm ruleAlgorithm = dataMappingConfig.ruleAlgorithm;
                        dataMappingEvaluator = new DataMappingEvaluator(rowSignature, dataMappingConfig.columnName, ruleAlgorithm);
                    } else {
                        dataMappingEvaluator = new DataMappingEvaluator(rowSignature);
                    }
                    currentSchema.add(tableName, new JdbcTable(schemaName, tableName, value,
                            CalciteConvertors.relDataType(columnInfos), rowSignature, dataMappingEvaluator
                    ));
                    LOGGER.error("build {}.{} success", schemaName, tableName);
                });
            });
            return calciteConnection;
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new RuntimeException(e);
        }
    }


    private void addSchema(String schemaName) {
        this.schemaBackendMetaMap.put(schemaName, new HashMap<>());
    }

    private void addTable(String schemaName, String tableName, List<BackEndTableInfo> tableInfos) {
        Map<String, List<BackEndTableInfo>> map = this.schemaBackendMetaMap.get(schemaName);
        map.put(tableName, tableInfos);
    }

    public static void main(String[] args) {

    }
}
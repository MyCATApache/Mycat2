package io.mycat.calcite;

import io.mycat.ConfigRuntime;
import io.mycat.config.ConfigFile;
import io.mycat.router.RuleAlgorithm;
import io.mycat.router.function.PartitionRuleAlgorithmManager;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.util.ConversionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Weiqing Xu
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
        System.setProperty("saffron.default.charset",charset);
        System.setProperty("saffron.default.nationalcharset",charset);
        System.setProperty("saffron.default.collat​​ion.name",charset +"$ en_US");
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
        PartitionRuleAlgorithmManager.INSTANCE.initFunctions(ConfigRuntime.INSTCANE.load().getConfig(ConfigFile.FUNCTIONS));
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


            Connection connection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false");
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
                        dataMappingEvaluator = new DataMappingEvaluator(rowSignature,dataMappingConfig.columnName, ruleAlgorithm);
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
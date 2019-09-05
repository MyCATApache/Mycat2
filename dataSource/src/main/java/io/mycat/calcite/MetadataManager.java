package io.mycat.calcite;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    MetadataManager() {
        addSchema("TESTDB");
        List<BackEndTableInfo> tableInfos = Arrays.asList(
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db1").tableName("TRAVELRECORD").build(),
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db1").tableName("TRAVELRECORD2").build(),
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db1").tableName("TRAVELRECORD3").build(),

                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db2").tableName("TRAVELRECORD").build(),
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db2").tableName("TRAVELRECORD2").build(),
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db2").tableName("TRAVELRECORD3").build(),

                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db3").tableName("TRAVELRECORD").build(),
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db3").tableName("TRAVELRECORD2").build(),
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db3").tableName("TRAVELRECORD3").build()
        );
        addTable("TESTDB", "TRAVELRECORD", tableInfos);
//        addTableDataMapping("TESTDB", "TRAVELRECORD", tableInfos);



        List<BackEndTableInfo> tableInfos2 = Arrays.asList(
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db1").tableName("address").build(),
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db2").tableName("address").build(),
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db3").tableName("address").build()
        );

        addTable("TESTDB", "ADDRESS", tableInfos2);

        if (schemaColumnMetaMap.isEmpty()) {
            schemaColumnMetaMap.putAll(CalciteConvertors.columnInfoList(schemaBackendMetaMap));
        }
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
                    currentSchema.add(tableName, new JdbcTable(schemaName, tableName, value,
                            CalciteConvertors.relDataType(columnInfos),
                            CalciteConvertors.rowSignature(columnInfos)));
                    LOGGER.error("build {}.{} success", schemaName, tableName);
                });
            });
            return calciteConnection;
        } catch (Exception e) {
            LOGGER.error("",e);
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
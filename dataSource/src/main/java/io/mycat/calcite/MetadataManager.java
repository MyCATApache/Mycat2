package io.mycat.calcite;

import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
public class MetadataManager {
    final Map<String, Map<String, List<BackEndTableInfo>>> schemaMetaMap = new ConcurrentHashMap<>();
    final Map<String, AbstractSchema> schemaMap = new ConcurrentHashMap<>();

    public MetadataManager() {
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

        List<BackEndTableInfo> tableInfos2 = Arrays.asList(
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db1").tableName("address").build(),
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db2").tableName("address").build(),
                BackEndTableInfo.builder().hostname("mytest3306a").schemaName("db3").tableName("address").build()
        );

        addTable("TESTDB", "address", tableInfos2);
    }

    private void addTable(String schemaName, String tableName, List<BackEndTableInfo> tableInfos) {
        Map<String, List<BackEndTableInfo>> map = this.schemaMetaMap.get(schemaName);
        map.put(tableName, tableInfos);
    }

    private void addSchema(String schemaName) {
        this.schemaMetaMap.put(schemaName, new HashMap<>());
    }

    public CalciteConnection getConnection() throws Exception {
        Connection connection = DriverManager.getConnection("jdbc:calcite:");
        CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        for (Map.Entry<String, Map<String, List<BackEndTableInfo>>> entry : schemaMetaMap.entrySet()) {
            String schemaName = entry.getKey();
            SchemaPlus currentSchema = rootSchema.add(schemaName, new AbstractSchema());
            Map<String, List<BackEndTableInfo>> tables = entry.getValue();
            boolean first = true;
            for (Map.Entry<String, List<BackEndTableInfo>> listEntry : tables.entrySet()) {
                String tableName = listEntry.getKey();
                List<BackEndTableInfo> list = listEntry.getValue();
                if (list.isEmpty()) {
                    throw new UnsupportedOperationException();
                }
                BackEndTableInfo tableInfo = list.get(0);
                List<CalciteConvertors.SimpleColumnInfo> infos;
                RelProtoDataType relProtoDataType = null;
                RowSignature rowSignature = null;
                if (first) {
                    first = false;
                    infos = getColumnInfo(tableInfo);
                    relProtoDataType = CalciteConvertors.relDataType(infos);
                    rowSignature = CalciteConvertors.rowSignature(infos);
                }
                Objects.requireNonNull(relProtoDataType);
                Objects.requireNonNull(rowSignature);
                currentSchema.add(tableName, new JdbcTable(schemaName, tableName, listEntry.getValue(),relProtoDataType,rowSignature));
                Map<String, List<BackEndTableInfo>> backend = entry.getValue();
            }
        }
        return calciteConnection;
    }

    private List<CalciteConvertors.SimpleColumnInfo> getColumnInfo(BackEndTableInfo tableInfo) throws SQLException {
        List<CalciteConvertors.SimpleColumnInfo> infos;
        JdbcDataSource datasource = GRuntime.INSTACNE.getJdbcDatasourceByName(tableInfo.getHostname());
        DefaultConnection defaultConnection = (DefaultConnection) datasource.getReplica().getDefaultConnection(datasource);
        try (Connection rawConnection = defaultConnection.getRawConnection()) {
            DatabaseMetaData metaData = rawConnection.getMetaData();
            String schema = tableInfo.getSchemaName();
            infos = CalciteConvertors.convertfromDatabaseMetaData(metaData, schema, schema, tableInfo.getTableName());
        }
        return infos;
    }
}
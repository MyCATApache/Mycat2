package io.mycat.calcite;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum  CalciteEnvironment {
    INSTANCE;
    final Logger LOGGER = LoggerFactory.getLogger(CalciteEnvironment.class);
    final ConcurrentHashMap<String, Map<String, JdbcTable>> logicTableMap;

     CalciteEnvironment() {
        this(null);
    }

    private CalciteEnvironment(ConcurrentHashMap<String, Map<String, JdbcTable>> logicTableMap) {
        this.logicTableMap = logicTableMap;
        final String charset = "UTF-8";
        System.setProperty("saffron.default.charset", charset);
        System.setProperty("saffron.default.nationalcharset", charset);
        System.setProperty("saffron.default.collat​​ion.tableName", charset + "$ en_US");
    }

    public CalciteConnection getConnection() {
        try {
            Connection connection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false;lex=MYSQL;fun=mysql;conformance=MYSQL_5");
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            SchemaPlus rootSchema = calciteConnection.getRootSchema();
            rootSchema.setCacheEnabled(true);
            setSchemaMap(rootSchema);
            return calciteConnection;
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new RuntimeException(e);
        }
    }
    public CalciteConnection getRawConnection() {
        try {
            Connection connection = DriverManager.getConnection("jdbc:calcite:caseSensitive=false;lex=MYSQL;fun=mysql;conformance=MYSQL_5");
            CalciteConnection calciteConnection = connection.unwrap(CalciteConnection.class);
            return calciteConnection;
        } catch (Exception e) {
            LOGGER.error("", e);
            throw new RuntimeException(e);
        }
    }
    public void setSchemaMap(SchemaPlus rootSchema) {
        Map<String, Map<String, JdbcTable>> schemaMap = getTableMap();
        for (Map.Entry<String, Map<String, JdbcTable>> e : schemaMap.entrySet()) {
            String k = e.getKey();
            Map<String, JdbcTable> v = e.getValue();
            SchemaPlus schemaPlus = rootSchema.add(k, new AbstractSchema());
            for (Map.Entry<String, JdbcTable> entry : v.entrySet()) {
                String t = entry.getKey();
                JdbcTable j = entry.getValue();
                schemaPlus.add(t, j);
            }
        }
    }

    public Map<String, Map<String, JdbcTable>> getTableMap() {
        return logicTableMap;
    }
}
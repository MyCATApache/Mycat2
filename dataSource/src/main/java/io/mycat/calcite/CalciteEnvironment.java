/**
 * Copyright (C) <2020>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.jdbc.CalcitePrepare;
import org.apache.calcite.jdbc.Driver;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public enum  CalciteEnvironment {
    INSTANCE;
    final Logger LOGGER = LoggerFactory.getLogger(CalciteEnvironment.class);


    private CalciteEnvironment() {
        Driver driver = new Driver();//触发驱动注册
        final String charset = "UTF-8";
        System.setProperty("saffron.default.charset", charset);
        System.setProperty("saffron.default.nationalcharset", charset);
        System.setProperty("calcite.default.charset",charset);
        System.setProperty("saffron.default.collat​​ion.tableName", charset + "$ en_US");
    }

    public CalciteConnection getConnection(MetadataManager metadataManager) {
        try {
            CalciteConnection rawConnection = getRawConnection();
            SchemaPlus rootSchema = rawConnection.getRootSchema();
            init(rootSchema,metadataManager);
            return rawConnection;
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
    public void init(SchemaPlus rootSchema,MetadataManager metadataManager) {
        for (Map.Entry<String, ConcurrentHashMap<String, MetadataManager.LogicTable>> stringConcurrentHashMapEntry : metadataManager.logicTableMap.entrySet()) {
            SchemaPlus schemaPlus = rootSchema.add(stringConcurrentHashMapEntry.getKey(), new AbstractSchema());
            for (Map.Entry<String, MetadataManager.LogicTable> entry : stringConcurrentHashMapEntry.getValue().entrySet()) {
                String t = entry.getKey();
                JdbcTable j = entry.getValue().getJdbcTable();
                schemaPlus.add(t, j);
            }
        }
    }
}
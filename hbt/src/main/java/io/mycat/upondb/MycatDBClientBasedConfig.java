package io.mycat.upondb;

import io.mycat.metadata.TableHandler;
import lombok.Builder;
import lombok.Getter;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

@Getter
@Builder
public class MycatDBClientBasedConfig {
    final Map<String, Map<String, TableHandler>> logicTables;
    final Map<String, Object> reflectiveSchemas;

    public MycatDBClientBasedConfig(Map<String, Map<String, TableHandler>> logicTables, Map<String, Object> reflectiveSchemas) {
        if (logicTables == null) {
            logicTables = Collections.emptyMap();
        }
        if (reflectiveSchemas == null) {
            reflectiveSchemas = Collections.emptyMap();
        }
        this.logicTables = logicTables;
        this.reflectiveSchemas = reflectiveSchemas;
    }

    public TableHandler getTable(String schema, String table) {
        Map<String, TableHandler> stringTableHandlerMap = logicTables.get(schema);
        Objects.requireNonNull(stringTableHandlerMap, "schema is not existed");
        TableHandler tableHandler = stringTableHandlerMap.get(table.toLowerCase());
        return Objects.requireNonNull(tableHandler, "table is not existed");
    }
}
package io.mycat.upondb;

import io.mycat.metadata.SchemaHandler;
import io.mycat.TableHandler;
import lombok.Builder;
import lombok.Getter;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

@Getter
@Builder
public class MycatDBClientBasedConfig {
    final Map<String, SchemaHandler> schemaMap;
    final Map<String, Object> reflectiveSchemas;
    private boolean cache;

    public MycatDBClientBasedConfig(Map<String, SchemaHandler> schemaMap,
                                    Map<String, Object> reflectiveSchemas,
                                    boolean cache) {
        this.cache = cache;
        if (schemaMap == null) {
            schemaMap = Collections.emptyMap();
        }
        if (reflectiveSchemas == null) {
            reflectiveSchemas = Collections.emptyMap();
        }
        this.schemaMap = schemaMap;
        this.reflectiveSchemas = reflectiveSchemas;
    }

    public TableHandler getTable(String schema, String table) {
        SchemaHandler stringTableHandlerMap = schemaMap.get(schema);
        Objects.requireNonNull(stringTableHandlerMap, "schema is not existed");
        TableHandler tableHandler = stringTableHandlerMap.logicTables().get(table);
        return Objects.requireNonNull(tableHandler, "table is not existed");
    }

    public boolean isCache() {
        return cache;
    }
}
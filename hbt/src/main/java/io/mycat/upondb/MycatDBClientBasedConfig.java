package io.mycat.upondb;

import io.mycat.metadata.LogicTable;
import lombok.Builder;
import lombok.Getter;

import java.util.Collections;
import java.util.Map;

@Getter
@Builder
public class MycatDBClientBasedConfig {
    final Map<String, Map<String, LogicTable>> logicTables;
    final Map<String,Object> reflectiveSchemas;

    public MycatDBClientBasedConfig(Map<String, Map<String, LogicTable>> logicTables, Map<String,Object> reflectiveSchemas) {
        if (logicTables == null) {
            logicTables = Collections.emptyMap();
        }
        if (reflectiveSchemas == null) {
            reflectiveSchemas = Collections.emptyMap();
        }
        this.logicTables = logicTables;
        this.reflectiveSchemas = reflectiveSchemas;
    }
}
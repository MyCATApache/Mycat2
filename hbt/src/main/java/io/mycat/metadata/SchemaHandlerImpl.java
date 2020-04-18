package io.mycat.metadata;

import java.util.HashMap;
import java.util.Map;

public class SchemaHandlerImpl implements SchemaHandler {
    final Map<String, TableHandler> tableMap = new HashMap<>();
    final String defaultTargetName;

    public SchemaHandlerImpl(String defaultTargetName) {
        this.defaultTargetName = defaultTargetName;
    }

    @Override
    public Map<String, TableHandler> logicTables() {
        return tableMap;
    }

    @Override
    public String defaultTargetName() {
        return defaultTargetName;
    }
}
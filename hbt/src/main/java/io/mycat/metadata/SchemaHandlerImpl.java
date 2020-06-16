package io.mycat.metadata;

import io.mycat.TableHandler;

import java.util.HashMap;
import java.util.Map;

public class SchemaHandlerImpl implements SchemaHandler {
    final Map<String, TableHandler> tableMap = new HashMap<>();
    private String name;
    final String defaultTargetName;

    public SchemaHandlerImpl(String name,String defaultTargetName) {
        this.name = name;
        this.defaultTargetName = defaultTargetName;
    }

    @Override
    public String getName() {
        return name;
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
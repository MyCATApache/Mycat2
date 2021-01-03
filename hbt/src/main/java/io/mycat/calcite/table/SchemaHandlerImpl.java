package io.mycat.calcite.table;

import io.mycat.TableHandler;
import io.mycat.util.NameMap;

public class SchemaHandlerImpl implements SchemaHandler {
    final NameMap<TableHandler> tableMap = new NameMap<>();
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
    public NameMap< TableHandler> logicTables() {
        return tableMap;
    }

    @Override
    public String defaultTargetName() {
        return defaultTargetName;
    }
}
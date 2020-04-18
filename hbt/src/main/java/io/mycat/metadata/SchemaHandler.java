package io.mycat.metadata;

import java.util.Map;

public interface SchemaHandler {
    Map<String, TableHandler> logicTables();

    String defaultTargetName();
}
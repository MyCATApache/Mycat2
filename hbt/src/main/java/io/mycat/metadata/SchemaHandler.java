package io.mycat.metadata;

import java.util.Map;

public interface SchemaHandler {
    String getName();

    Map<String, TableHandler> logicTables();

    String defaultTargetName();
}
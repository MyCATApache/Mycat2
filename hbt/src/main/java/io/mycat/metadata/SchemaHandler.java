package io.mycat.metadata;

import io.mycat.TableHandler;

import java.util.Map;

public interface SchemaHandler {
    String getName();

    Map<String, TableHandler> logicTables();

    String defaultTargetName();
}
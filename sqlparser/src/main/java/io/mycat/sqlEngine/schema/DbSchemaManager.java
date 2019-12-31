package io.mycat.sqlEngine.schema;

import io.mycat.sqlEngine.executor.logicExecutor.ExecutorType;
import io.mycat.sqlEngine.executor.logicExecutor.LogicLeafTableExecutor;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum DbSchemaManager {
  INSTANCE;
  final ConcurrentMap<String, DbSchema> schemas = new ConcurrentHashMap<>();

    public DbConsole createConsole() {
    return new DbConsole();
  }

  public LogicLeafTableExecutor getLogicLeafTableSource(String schema,
                                                        String tableName,
                                                        TableColumnDefinition[] columnDefinitions, long offset, long rowCount, ExecutorType type) {
    DbTable table = Objects.requireNonNull(schemas.get(schema).getTableByName(tableName));
    return new LogicLeafTableExecutor(columnDefinitions, table, new HashMap<>(), type);
  }

  public DbTable getTable(String schemaName, String tableName) {
    return schemas.get(schemaName).getTableByName(tableName);
  }
}
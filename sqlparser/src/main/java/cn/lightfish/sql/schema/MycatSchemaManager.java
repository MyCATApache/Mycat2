package cn.lightfish.sql.schema;

import cn.lightfish.sql.executor.logicExecutor.ExecutorType;
import cn.lightfish.sql.executor.logicExecutor.LogicLeafTableExecutor;

import java.util.HashMap;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum MycatSchemaManager {
  INSTANCE;
  final ConcurrentMap<String, MycatSchema> schemas = new ConcurrentHashMap<>();

  MycatConsole createConsole() {
    return new MycatConsole();
  }

  public LogicLeafTableExecutor getLogicLeafTableSource(String schema,
                                                        String tableName,
                                                        TableColumnDefinition[] columnDefinitions, long offset, long rowCount, ExecutorType type) {
    MycatTable table = Objects.requireNonNull(schemas.get(schema).getTableByName(tableName));
    return new LogicLeafTableExecutor(columnDefinitions, table, new HashMap<>(), type);
  }

  public MycatTable getTable(String schemaName, String tableName) {
    return schemas.get(schemaName).getTableByName(tableName);
  }
}
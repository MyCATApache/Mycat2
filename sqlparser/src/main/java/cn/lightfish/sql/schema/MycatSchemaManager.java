package cn.lightfish.sql.schema;

import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.executor.logicExecutor.LogicLeafTableExecutor;
import java.util.HashMap;
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
      TableColumnDefinition[] columnDefinitions, long offset, long rowCount) {
    MycatTable table = schemas.get(schema).getTableByName(tableName);
    return new LogicLeafTableExecutor(columnDefinitions, table, new HashMap<>());
  }

  public MycatTable getTable(String schemaName, String tableName) {
    return schemas.get(schemaName).getTableByName(tableName);
  }
}
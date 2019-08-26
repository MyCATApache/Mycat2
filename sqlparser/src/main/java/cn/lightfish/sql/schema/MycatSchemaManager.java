package cn.lightfish.sql.schema;

import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.executor.DefExecutor;
import cn.lightfish.sql.executor.logicExecutor.LogicTableExecutor;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum MycatSchemaManager {
  INSTANCE;
  final ConcurrentMap<String, MycatSchema> schemas = new ConcurrentHashMap<>();

  MycatConsole createConsole() {
   return new MycatConsole();
  }

  public LogicTableExecutor getLogicTableSource(RootSessionContext context,String schema, String tableName,
      SimpleColumnDefinition[] columnDefinitions, long offset, long rowCount) {
    MycatTable table = schemas.get(schema).getTableByName(tableName);
    LogicTableExecutor logicTableExecutor = new LogicTableExecutor(columnDefinitions, table);
    List<Object[]> list = Arrays.asList(new Object[]{1L}, new Object[]{2L});
    logicTableExecutor.setPhysicsExecutor(new DefExecutor(columnDefinitions,list.iterator()));
    return  logicTableExecutor;
  }
}
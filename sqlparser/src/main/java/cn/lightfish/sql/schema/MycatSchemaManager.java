package cn.lightfish.sql.schema;

import cn.lightfish.sql.executor.Executor;
import cn.lightfish.sql.executor.SimpleExecutor;
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

  public Executor getTableSource(String schema, String tableName,
      MycatColumnDefinition[] columnDefinitions, long offset, long rowCount) {
    MycatTable table = schemas.get(schema).getTableByName(tableName);
    List<Object[]> list = Arrays.asList(new Object[]{1L}, new Object[]{2L});
    return new SimpleExecutor(columnDefinitions,list.iterator());
  }
}
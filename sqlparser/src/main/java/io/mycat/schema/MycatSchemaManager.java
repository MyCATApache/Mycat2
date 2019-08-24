package io.mycat.schema;

import cn.lightfish.sql.ast.Executor;
import cn.lightfish.sql.ast.SimpleExecutor;
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

  public Executor getTableSource(String schema, String tableName) {
    MycatTable table = schemas.get(schema).getTableByName(tableName);
    List<MycatColumnDefinition> columnDefinitions = table.columnDefinitions;
    List<Object[]> list = Arrays.asList(new Object[]{1L}, new Object[]{2L});
    return new SimpleExecutor(columnDefinitions.toArray( new MycatColumnDefinition[0]),list.iterator());
  }
}
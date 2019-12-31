package io.mycat.sqlEngine.persistent.impl;

import cn.lightfish.sqlEngine.persistent.*;
import io.mycat.sqlEngine.persistent.*;
import io.mycat.sqlEngine.schema.DbTable;
import io.mycat.sqlEngine.schema.TableColumnDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultPersistentImpl implements Persistent {


  final List<Object[]> rows = new ArrayList<>();
  private DbTable table;

  public DefaultPersistentImpl(DbTable table) {
    this.table = table;
  }

  @Override
  public InsertPersistent createInsertPersistent(TableColumnDefinition[] columnNameList,
                                                 Map<String, Object> persistentAttributes) {
    return new InsertPersistent(table,rows);
  }

  @Override
  public QueryPersistent createQueryPersistent(TableColumnDefinition[] columnNameList,
                                               Map<String, Object> persistentAttributes) {
    return new QueryPersistentImpl(table,rows.iterator());
  }

  @Override
  public UpdatePersistent createUpdatePersistent(TableColumnDefinition[] columnNameList, Map<String, Object> persistentAttributes) {
    return new UpdatePersistent(table,rows.iterator());
  }
}
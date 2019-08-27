package cn.lightfish.sql.persistent.impl;

import cn.lightfish.sql.persistent.InsertPersistent;
import cn.lightfish.sql.persistent.Persistent;
import cn.lightfish.sql.persistent.QueryPersistent;
import cn.lightfish.sql.persistent.UpdatePersistent;
import cn.lightfish.sql.schema.MycatTable;
import cn.lightfish.sql.schema.SimpleColumnDefinition;
import cn.lightfish.sql.schema.TableColumnDefinition;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DefaultPersistentImpl implements Persistent {


  final List<Object[]> rows = new ArrayList<>();
  private MycatTable table;

  public DefaultPersistentImpl(MycatTable table) {
    this.table = table;
  }

  @Override
  public InsertPersistent createInsertPersistent(TableColumnDefinition[] columnNameList,
      Map<String, Object> persistentAttributes) {
    for (SimpleColumnDefinition columnDefinition : table.getColumnDefinitions()) {
      table.getColumnDefinitions();
      columnDefinition.getColumnName();
    }

    return null;
  }

  @Override
  public QueryPersistent createQueryPersistent(TableColumnDefinition[] columnNameList,
      Map<String, Object> persistentAttributes) {
    return null;
  }

  @Override
  public UpdatePersistent createUpdatePersistent(TableColumnDefinition[] columnNameList,
      Map<String, Object> persistentAttributes) {
    return null;
  }
}
package io.mycat.sqlEngine.persistent.impl;

import io.mycat.sqlEngine.persistent.Persistent;
import io.mycat.sqlEngine.persistent.PersistentProvider;
import io.mycat.sqlEngine.schema.DbTable;

import java.util.Map;

public class DefaultPersistentProvider implements PersistentProvider {

  @Override
  public Persistent create(DbTable table, Map<String, Object> persistentAttributes) {
    return new DefaultPersistentImpl(table);
  }
}
package io.mycat.sqlEngine.persistent;

import io.mycat.sqlEngine.schema.DbTable;

import java.util.Map;

public interface PersistentProvider {
  public Persistent create(DbTable table, Map<String, Object> persistentAttributes);
}
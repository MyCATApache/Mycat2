package cn.lightfish.sqlEngine.persistent;

import cn.lightfish.sqlEngine.schema.DbTable;
import java.util.Map;

public interface PersistentProvider {
  public Persistent create(DbTable table, Map<String, Object> persistentAttributes);
}
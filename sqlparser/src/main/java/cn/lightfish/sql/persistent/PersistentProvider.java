package cn.lightfish.sql.persistent;

import cn.lightfish.sql.schema.MycatTable;
import java.util.Map;

public interface PersistentProvider {
  public Persistent create(MycatTable table, Map<String, Object> persistentAttributes);
}
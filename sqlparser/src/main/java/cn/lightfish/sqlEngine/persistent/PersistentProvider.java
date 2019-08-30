package cn.lightfish.sqlEngine.persistent;

import cn.lightfish.sqlEngine.schema.MycatTable;
import java.util.Map;

public interface PersistentProvider {
  public Persistent create(MycatTable table, Map<String, Object> persistentAttributes);
}
package cn.lightfish.sql.persistent;

import cn.lightfish.sql.schema.MycatConsole;
import cn.lightfish.sql.schema.MycatTable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public enum PersistentManager {
  INSTANCE;
  ConcurrentMap<MycatTable, Persistent> map = new ConcurrentHashMap<>();

  public InsertPersistent getInsertPersistent(MycatConsole console,
      MycatTable table, String[] columnNameList,
      Map<String, Object> persistentAttribute) {
    return null;
  }

  static class Persistent {

  }
}
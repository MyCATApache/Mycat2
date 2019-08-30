package cn.lightfish.sqlEngine.persistent.impl;

import cn.lightfish.sqlEngine.persistent.Persistent;
import cn.lightfish.sqlEngine.persistent.PersistentProvider;
import cn.lightfish.sqlEngine.schema.MycatTable;

import java.util.Map;

public class DefaultPersistentProvider implements PersistentProvider {

  @Override
  public Persistent create(MycatTable table, Map<String, Object> persistentAttributes) {
    return new DefaultPersistentImpl(table);
  }
}
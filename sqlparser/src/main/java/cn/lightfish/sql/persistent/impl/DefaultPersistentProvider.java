package cn.lightfish.sql.persistent.impl;

import cn.lightfish.sql.persistent.Persistent;
import cn.lightfish.sql.persistent.PersistentProvider;
import cn.lightfish.sql.schema.MycatTable;
import io.mycat.beans.mycat.MycatSchema;
import java.util.Map;

public class DefaultPersistentProvider implements PersistentProvider {

  @Override
  public Persistent create(MycatTable table, Map<String, Object> persistentAttributes) {
    return new DefaultPersistentImpl(table);
  }
}
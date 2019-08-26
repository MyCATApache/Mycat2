package cn.lightfish.sql.executor;

import cn.lightfish.sql.schema.MycatColumnDefinition;

public interface Executor {

  MycatColumnDefinition[] columnDefList();

  boolean hasNext();

  Object[] next();
}
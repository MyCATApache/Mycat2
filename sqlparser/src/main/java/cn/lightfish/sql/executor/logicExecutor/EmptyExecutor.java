package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.schema.SimpleColumnDefinition;

public enum  EmptyExecutor implements Executor {
INSTANCE;

  @Override
  public SimpleColumnDefinition[] columnDefList() {
    return new SimpleColumnDefinition[0];
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Object[] next() {
    return new Object[0];
  }
}
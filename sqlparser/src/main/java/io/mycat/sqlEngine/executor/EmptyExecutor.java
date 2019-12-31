package io.mycat.sqlEngine.executor;

import io.mycat.sqlEngine.executor.logicExecutor.Executor;
import io.mycat.sqlEngine.schema.BaseColumnDefinition;

public enum EmptyExecutor implements Executor {
  INSTACNE;

  @Override
  public BaseColumnDefinition[] columnDefList() {
    return new BaseColumnDefinition[0];
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
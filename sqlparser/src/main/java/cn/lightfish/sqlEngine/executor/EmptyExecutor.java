package cn.lightfish.sqlEngine.executor;

import cn.lightfish.sqlEngine.executor.logicExecutor.Executor;
import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;

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
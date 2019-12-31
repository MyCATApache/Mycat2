package io.mycat.sqlEngine.executor.logicExecutor;

import io.mycat.sqlEngine.schema.BaseColumnDefinition;

public class LogicUpdateExecutor implements Executor {
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

  public void replace(Object[] row){

  }
}
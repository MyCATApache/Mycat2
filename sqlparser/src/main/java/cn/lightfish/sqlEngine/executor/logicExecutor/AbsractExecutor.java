package cn.lightfish.sqlEngine.executor.logicExecutor;

import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;

public abstract class AbsractExecutor implements Executor {

  protected final BaseColumnDefinition[] columnList;

  public AbsractExecutor(BaseColumnDefinition[] columnList) {
    this.columnList = columnList;
  }

  @Override
  public BaseColumnDefinition[] columnDefList() {
    return columnList;
  }
}
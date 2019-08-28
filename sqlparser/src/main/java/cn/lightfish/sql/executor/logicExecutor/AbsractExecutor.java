package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.schema.BaseColumnDefinition;

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
package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.schema.SimpleColumnDefinition;

public abstract class AbsractExecutor implements Executor {

  protected final SimpleColumnDefinition[] columnList;

  public AbsractExecutor(SimpleColumnDefinition[] columnList) {
    this.columnList = columnList;
  }

  @Override
  public SimpleColumnDefinition[] columnDefList() {
    return columnList;
  }
}
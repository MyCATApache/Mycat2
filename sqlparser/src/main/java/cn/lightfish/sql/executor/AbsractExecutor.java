package cn.lightfish.sql.executor;

import cn.lightfish.sql.schema.MycatColumnDefinition;

public abstract class AbsractExecutor implements Executor {

  protected final MycatColumnDefinition[] columnList;

  public AbsractExecutor(MycatColumnDefinition[] columnList) {
    this.columnList = columnList;
  }

  @Override
  public MycatColumnDefinition[] columnDefList() {
    return columnList;
  }
}
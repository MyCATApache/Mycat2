package cn.lightfish.sql.ast.executor;

import cn.lightfish.sql.ast.Executor;
import io.mycat.schema.MycatColumnDefinition;

public enum  EmptyExecutor implements Executor {
INSTANCE;

  @Override
  public MycatColumnDefinition[] columnDefList() {
    return new MycatColumnDefinition[0];
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
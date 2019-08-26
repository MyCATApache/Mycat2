package cn.lightfish.sql.executor;

import cn.lightfish.sql.schema.MycatColumnDefinition;

public class OnlyProjectExecutor extends AbsractExecutor {

  Object[] objectList;

  public OnlyProjectExecutor(MycatColumnDefinition[] columnList, Object[] objectList) {
    super(columnList);
    this.objectList = objectList;
  }

  @Override
  public boolean hasNext() {
    return objectList != null;
  }
  @Override
  public Object[] next() {
    try {
      return objectList;
    } finally {
      objectList = null;
    }
  }
}
package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.schema.SimpleColumnDefinition;

public class OnlyProjectExecutor extends AbsractExecutor {

  Object[] objectList;

  public OnlyProjectExecutor(SimpleColumnDefinition[] columnList, Object[] objectList) {
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
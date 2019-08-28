package cn.lightfish.sql.executor.logicExecutor;

import cn.lightfish.sql.schema.BaseColumnDefinition;

public class OnlyProjectExecutor extends AbsractExecutor {

  Object[] objectList;

  public OnlyProjectExecutor(BaseColumnDefinition[] columnList, Object[] objectList) {
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
package cn.lightfish.sqlEngine.executor.logicExecutor;

import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;

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
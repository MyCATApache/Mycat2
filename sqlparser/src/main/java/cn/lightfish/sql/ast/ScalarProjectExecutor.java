package cn.lightfish.sql.ast;

import io.mycat.schema.MycatColumnDefinition;

public class ScalarProjectExecutor extends AbsractExecutor {

  Object[] objectList;

  public ScalarProjectExecutor(MycatColumnDefinition[] columnList, Object[] objectList) {
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
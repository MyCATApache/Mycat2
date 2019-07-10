package io.mycat.compute.impl;

import io.mycat.compute.Expression;
import io.mycat.compute.OperateType;

public class ExpressionTable extends Expression<ExpressionTable> {

  public ExpressionTable(OperateType operateType) {
    super(operateType);
  }

  @Override
  public ExpressionTable clone() {
    return null;
  }

  @Override
  public OperateType getOperateType() {
    return OperateType.TABLE;
  }

}
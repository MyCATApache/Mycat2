package io.mycat.sqlEngine.ast.expr.numberExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;

public interface DoubleExpr extends ValueExpr<Double> {

  @Override
  default public Class<Double> getType() {
    return Double.class;
  }
}
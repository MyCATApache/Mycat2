package cn.lightfish.sqlEngine.ast.expr.numberExpr;

import cn.lightfish.sqlEngine.ast.expr.ValueExpr;

public interface DoubleExpr extends ValueExpr<Double> {

  @Override
  default public Class<Double> getType() {
    return Double.class;
  }
}
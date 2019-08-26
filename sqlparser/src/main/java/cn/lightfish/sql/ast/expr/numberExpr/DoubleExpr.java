package cn.lightfish.sql.ast.expr.numberExpr;

import cn.lightfish.sql.ast.expr.ValueExpr;

public interface DoubleExpr extends ValueExpr<Double> {

  @Override
  default public Class<Double> getType() {
    return Double.class;
  }
}
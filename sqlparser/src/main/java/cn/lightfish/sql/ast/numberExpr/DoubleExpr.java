package cn.lightfish.sql.ast.numberExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;

public interface DoubleExpr extends ValueExpr<Double> {

  @Override
  default public Class<Double> getType() {
    return Double.class;
  }
}
package io.mycat.sqlEngine.ast.expr.numberExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;

import java.math.BigDecimal;

public interface BigDecimalExpr extends ValueExpr<BigDecimal> {

  @Override
  default public Class<BigDecimal> getType() {
    return BigDecimal.class;
  }
}
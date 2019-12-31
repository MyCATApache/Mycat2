package io.mycat.sqlEngine.ast.expr.numberExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;

public interface LongExpr extends ValueExpr<Long> {
  @Override
  default public Class<Long> getType() {
    return Long.class;
  }
}
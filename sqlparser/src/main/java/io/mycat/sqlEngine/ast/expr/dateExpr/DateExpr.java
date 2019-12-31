package io.mycat.sqlEngine.ast.expr.dateExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;

public interface DateExpr extends ValueExpr<java.util.Date> {

  @Override
  default public Class<java.util.Date> getType() {
    return java.util.Date.class;
  }
}
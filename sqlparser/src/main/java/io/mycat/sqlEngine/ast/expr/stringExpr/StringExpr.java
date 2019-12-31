package io.mycat.sqlEngine.ast.expr.stringExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;

public interface StringExpr extends ValueExpr<String> {
  @Override
 default public Class<String> getType() {
    return String.class;
  }
}
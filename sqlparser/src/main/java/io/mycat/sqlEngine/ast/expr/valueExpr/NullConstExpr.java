package io.mycat.sqlEngine.ast.expr.valueExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;

public enum NullConstExpr implements ValueExpr {
  INSTANCE;

  @Override
  public Class<Void> getType() {
    return Void.TYPE;
  }

  @Override
  public Comparable getValue() {
    return null;
  }
}
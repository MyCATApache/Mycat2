package cn.lightfish.sql.ast.expr.valueExpr;

import cn.lightfish.sql.ast.expr.ValueExpr;

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
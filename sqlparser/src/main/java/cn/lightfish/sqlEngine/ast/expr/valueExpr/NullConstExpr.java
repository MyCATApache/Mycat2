package cn.lightfish.sqlEngine.ast.expr.valueExpr;

import cn.lightfish.sqlEngine.ast.expr.ValueExpr;

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
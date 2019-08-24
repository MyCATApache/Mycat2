package cn.lightfish.sql.ast.valueExpr;

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
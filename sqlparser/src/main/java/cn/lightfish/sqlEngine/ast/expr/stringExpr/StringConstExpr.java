package cn.lightfish.sqlEngine.ast.expr.stringExpr;

public class StringConstExpr implements StringExpr{
  final String value;

  public StringConstExpr(String value) {
    this.value = value;
  }

  @Override
  public String getValue() {
    return value;
  }
}
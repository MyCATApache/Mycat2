package cn.lightfish.sql.ast.stringExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;

public class StringConstExpr implements ValueExpr<String> {
  final String value;

  public StringConstExpr(String value) {
    this.value = value;
  }

  @Override
  public String getValue() {
    return value;
  }

  @Override
  public Class<String> getType() {
    return String.class;
  }
}
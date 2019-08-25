package cn.lightfish.sql.ast.stringExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;

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
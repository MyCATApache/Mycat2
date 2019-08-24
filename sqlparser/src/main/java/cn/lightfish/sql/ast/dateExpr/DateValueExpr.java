package cn.lightfish.sql.ast.dateExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;

public class DateValueExpr implements ValueExpr<String> {

  private String date;

  public DateValueExpr(String date) {
    this.date = date;
  }

  @Override
  public String getValue() {
    return date;
  }

  @Override
  public Class<String> getType() {
    return String.class;
  }
}
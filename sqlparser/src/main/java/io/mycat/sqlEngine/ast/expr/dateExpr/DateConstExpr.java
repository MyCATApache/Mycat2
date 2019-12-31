package io.mycat.sqlEngine.ast.expr.dateExpr;

import java.sql.Date;

public class DateConstExpr implements DateExpr {

  private Date date;
  public DateConstExpr(String date) {
    this.date = Date.valueOf(date);
  }
  public DateConstExpr(Date date) {
    this.date = date;
  }

  @Override
  public Date getValue() {
    return date;
  }
}
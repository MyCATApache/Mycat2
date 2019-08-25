package cn.lightfish.sql.ast.dateExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.util.Date;

public class DateValueExpr implements ValueExpr<Date> {

  private Date date;
  public DateValueExpr(String date) {
    this.date = new Date(Date.parse(date));
  }
  public DateValueExpr(Date date) {
    this.date = date;
  }

  @Override
  public Date getValue() {
    return date;
  }

  @Override
  public Class<Date> getType() {
    return Date.class;
  }
}
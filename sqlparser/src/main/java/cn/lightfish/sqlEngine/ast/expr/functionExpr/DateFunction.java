package cn.lightfish.sqlEngine.ast.expr.functionExpr;

import java.sql.Time;
import java.sql.Timestamp;

public class DateFunction {

  public static java.sql.Date current_date() {
    return new java.sql.Date(System.currentTimeMillis());
  }

  public static Time current_time() {
    return new Time(System.currentTimeMillis());
  }

  public static Timestamp current_timestamp() {
    return new Timestamp(System.currentTimeMillis());
  }

}
package cn.lightfish.sql.ast.dateExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.sql.Date;
import java.time.LocalDate;

public interface DateExpr extends ValueExpr {

  @Override
  default public Class<Date> getType() {
    return Date.class;
  }
}
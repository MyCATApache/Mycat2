package cn.lightfish.sql.ast.expr.dateExpr;

import cn.lightfish.sql.ast.expr.ValueExpr;

public interface DateExpr extends ValueExpr<java.util.Date> {

  @Override
  default public Class<java.util.Date> getType() {
    return java.util.Date.class;
  }
}
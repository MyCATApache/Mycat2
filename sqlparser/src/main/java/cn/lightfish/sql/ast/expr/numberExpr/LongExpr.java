package cn.lightfish.sql.ast.expr.numberExpr;

import cn.lightfish.sql.ast.expr.ValueExpr;

public interface LongExpr extends ValueExpr<Long> {
  @Override
  default public Class<Long> getType() {
    return Long.class;
  }
}
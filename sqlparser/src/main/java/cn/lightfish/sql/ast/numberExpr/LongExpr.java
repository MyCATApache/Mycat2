package cn.lightfish.sql.ast.numberExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;

public interface LongExpr extends ValueExpr<Long> {
  @Override
  default public Class<Long> getType() {
    return Long.class;
  }
}
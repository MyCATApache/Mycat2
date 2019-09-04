package cn.lightfish.sqlEngine.ast.expr.numberExpr;

import cn.lightfish.sqlEngine.ast.expr.ValueExpr;

public interface LongExpr extends ValueExpr<Long> {
  @Override
  default public Class<Long> getType() {
    return Long.class;
  }
}
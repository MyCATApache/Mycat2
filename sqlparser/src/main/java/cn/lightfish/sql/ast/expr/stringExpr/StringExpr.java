package cn.lightfish.sql.ast.expr.stringExpr;

import cn.lightfish.sql.ast.expr.ValueExpr;

public interface StringExpr extends ValueExpr<String> {
  @Override
 default public Class<String> getType() {
    return String.class;
  }
}
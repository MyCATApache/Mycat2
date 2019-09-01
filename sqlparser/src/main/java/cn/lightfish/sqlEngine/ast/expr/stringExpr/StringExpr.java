package cn.lightfish.sqlEngine.ast.expr.stringExpr;

import cn.lightfish.sqlEngine.ast.expr.ValueExpr;

public interface StringExpr extends ValueExpr<String> {
  @Override
 default public Class<String> getType() {
    return String.class;
  }
}
package cn.lightfish.sql.ast.stringExpr;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;

public interface StringExpr extends ValueExpr<String> {
  @Override
 default public Class<String> getType() {
    return String.class;
  }
}
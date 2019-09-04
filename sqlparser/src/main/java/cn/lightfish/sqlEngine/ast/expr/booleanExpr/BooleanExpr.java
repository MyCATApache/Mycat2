package cn.lightfish.sqlEngine.ast.expr.booleanExpr;

import cn.lightfish.sqlEngine.ast.expr.ValueExpr;

public interface BooleanExpr extends ValueExpr<Integer> {

  @Override
  default Class<Integer> getType() {
    return Integer.class;
  }

  default public Integer getValue() {
    Boolean test = test();
    if (test == Boolean.TRUE) {
      return 1;
    } else if (test == Boolean.FALSE) {
      return 0;
    } else {
      return null;
    }
  }

  public Boolean test();
}
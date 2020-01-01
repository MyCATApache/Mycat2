package io.mycat.sqlEngine.ast.expr.booleanExpr.compareExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import io.mycat.sqlEngine.context.RootSessionContext;


public class BooleanNotLikeExpr implements BooleanExpr {

  private final BooleanLikeExpr booleanLikeExpr;

  public BooleanNotLikeExpr(RootSessionContext context, ValueExpr expr,
      String pattern) {
    booleanLikeExpr = new BooleanLikeExpr(context, expr, pattern);
  }

  @Override
  public Boolean test() {
    Boolean test = booleanLikeExpr.test();
    if (test == null) {
      return null;
    }
    return !test;
  }
}
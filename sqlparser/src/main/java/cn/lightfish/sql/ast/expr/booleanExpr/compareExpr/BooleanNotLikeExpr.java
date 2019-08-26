package cn.lightfish.sql.ast.expr.booleanExpr.compareExpr;

import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;


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
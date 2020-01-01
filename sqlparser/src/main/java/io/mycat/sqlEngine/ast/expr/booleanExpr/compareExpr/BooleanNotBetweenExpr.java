package io.mycat.sqlEngine.ast.expr.booleanExpr.compareExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import io.mycat.sqlEngine.context.RootSessionContext;

public class BooleanNotBetweenExpr implements BooleanExpr {

  final BooleanBetweenExpr betweenExpr;

  public BooleanNotBetweenExpr(
      final RootSessionContext context,
      final ValueExpr expr,
      final ValueExpr left,
      final ValueExpr right) {
    this.betweenExpr = new BooleanBetweenExpr(context, expr, left, right);
  }

  @Override
  public Boolean test() {
    Boolean test = betweenExpr.test();
    if (test == null) {
      return test;
    }
    return !test;
  }
}
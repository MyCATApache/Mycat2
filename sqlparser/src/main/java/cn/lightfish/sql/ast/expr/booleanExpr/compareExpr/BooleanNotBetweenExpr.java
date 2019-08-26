package cn.lightfish.sql.ast.expr.booleanExpr.compareExpr;

import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;

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
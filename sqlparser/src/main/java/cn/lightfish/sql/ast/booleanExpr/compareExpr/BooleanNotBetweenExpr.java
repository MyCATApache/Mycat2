package cn.lightfish.sql.ast.booleanExpr.compareExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;

public class BooleanNotBetweenExpr implements BooleanExpr {

  final BooleanBetweenExpr betweenExpr;

  public BooleanNotBetweenExpr(
      final RootExecutionContext context,
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
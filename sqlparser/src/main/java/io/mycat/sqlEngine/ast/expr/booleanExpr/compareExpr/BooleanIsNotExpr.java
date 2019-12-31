package io.mycat.sqlEngine.ast.expr.booleanExpr.compareExpr;

import io.mycat.sqlEngine.context.RootSessionContext;
import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import io.mycat.sqlEngine.ast.expr.ValueExpr;


public class BooleanIsNotExpr implements BooleanExpr {

  final BooleanIsExpr booleanIsExpr;

  public BooleanIsNotExpr(RootSessionContext context,
      final ValueExpr expr,
      final ValueExpr target) {
    this.booleanIsExpr = new BooleanIsExpr(context, expr, target);
  }

  @Override
  public Boolean test() {
    Boolean test = this.booleanIsExpr.test();
    if (test == null) {
      return null;
    } else {
      return !test;
    }
  }
}
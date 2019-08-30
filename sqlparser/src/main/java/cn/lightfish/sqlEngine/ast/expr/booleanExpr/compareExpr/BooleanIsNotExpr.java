package cn.lightfish.sqlEngine.ast.expr.booleanExpr.compareExpr;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sqlEngine.ast.expr.ValueExpr;


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
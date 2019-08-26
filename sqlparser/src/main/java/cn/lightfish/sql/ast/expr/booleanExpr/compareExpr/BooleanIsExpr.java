package cn.lightfish.sql.ast.expr.booleanExpr.compareExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;


public class BooleanIsExpr implements BooleanExpr {

  private final RootExecutionContext context;
  private final ValueExpr expr;
  private final ValueExpr target;

  public BooleanIsExpr(RootExecutionContext context, ValueExpr expr,
      ValueExpr target) {
    this.context = context;
    this.expr = expr;
    this.target = target;
    if (this.target.getType() != Integer.class) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Boolean test() {
    Comparable test = this.expr.getValue();
    Comparable target = this.target.getValue();
    if (test == null && target == null) {
      return true;
    }
    if (test == null && target != null) {
      return false;
    }
    if (test != null && target == null) {
      return false;
    }
    if (test != null && target != null) {
      Number targetValue = (Number) target;
      if ((test instanceof Number) && (targetValue instanceof Number)) {
        int testValue = ((Number) test).intValue();
        return (testValue == 0 && targetValue.intValue() == 0);
      } else {
        return targetValue.equals(test);
      }
    }
    return false;
  }
}
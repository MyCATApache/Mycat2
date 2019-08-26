package cn.lightfish.sql.ast.expr.booleanExpr.compareExpr;

import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanBetweenExpr implements BooleanExpr {

  private final RootSessionContext context;
  private final ValueExpr expr;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Boolean test() {
    Comparable test = this.expr.getValue();
    Comparable leftValue = this.left.getValue();
    Comparable rightValue = this.right.getValue();
    if (test != null && leftValue != null && rightValue != null) {
      return leftValue.compareTo(test) <= 0 && test.compareTo(rightValue) <= 0;
    } else {
      return null;
    }
  }
}
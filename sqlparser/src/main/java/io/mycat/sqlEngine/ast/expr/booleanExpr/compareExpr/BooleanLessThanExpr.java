package io.mycat.sqlEngine.ast.expr.booleanExpr.compareExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import io.mycat.sqlEngine.context.RootSessionContext;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanLessThanExpr implements BooleanExpr {

  private final RootSessionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Boolean test() {
    Comparable leftValue = left.getValue();
    Comparable rightValue = right.getValue();
    if (leftValue != null && rightValue != null) {
      return leftValue.compareTo(rightValue) < 0;
    } else {
      return Boolean.FALSE;
    }
  }
}
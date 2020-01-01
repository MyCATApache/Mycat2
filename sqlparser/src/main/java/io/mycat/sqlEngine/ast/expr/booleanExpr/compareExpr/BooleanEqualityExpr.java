package io.mycat.sqlEngine.ast.expr.booleanExpr.compareExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import io.mycat.sqlEngine.context.RootSessionContext;

import java.util.Objects;

public class BooleanEqualityExpr implements BooleanExpr {

  private final RootSessionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  public BooleanEqualityExpr(RootSessionContext context, ValueExpr left,
      ValueExpr right) {
    this.context = context;
    this.left = left;
    this.right = right;
  }

  @Override
  public Boolean test() {
    Object leftValue = left.getValue();
    Object rightValue = right.getValue();
    if (leftValue == null) {
      return null;
    }
    if (rightValue == null) {
      return null;
    }
    return  Objects.equals(leftValue, rightValue);
  }
}
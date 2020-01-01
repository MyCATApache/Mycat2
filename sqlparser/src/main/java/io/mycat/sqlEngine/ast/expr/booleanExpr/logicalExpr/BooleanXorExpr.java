package io.mycat.sqlEngine.ast.expr.booleanExpr.logicalExpr;

import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import io.mycat.sqlEngine.context.RootSessionContext;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanXorExpr implements BooleanExpr {

  private final RootSessionContext context;
  private final BooleanExpr left;
  private final BooleanExpr right;

  @Override
  public Boolean test() {
    Boolean leftValue = (Boolean) left.test();
    if (leftValue == null) {
      return false;
    }
    Boolean rightValue = (Boolean) right.test();
    if (rightValue == null) {
      return false;
    }
    return Boolean.logicalXor(leftValue,rightValue);
  }
}
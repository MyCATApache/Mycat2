package io.mycat.sqlEngine.ast.expr.arithmeticExpr.longOperator;

import io.mycat.sqlEngine.context.RootSessionContext;
import io.mycat.sqlEngine.ast.expr.numberExpr.LongExpr;
import io.mycat.sqlEngine.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LongModExpr implements LongExpr {

  private final RootSessionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Long getValue() {
    Long leftValue = (Long) this.left.getValue();
    if (leftValue == null) {
      return null;
    }
    Long rightValue = (Long) this.right.getValue();
    if (rightValue == null) {
      return rightValue;
    }
    return leftValue % rightValue;
  }
}
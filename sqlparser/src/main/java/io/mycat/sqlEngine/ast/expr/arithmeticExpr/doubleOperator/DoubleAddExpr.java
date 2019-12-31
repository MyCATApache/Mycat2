package io.mycat.sqlEngine.ast.expr.arithmeticExpr.doubleOperator;

import io.mycat.sqlEngine.context.RootSessionContext;
import io.mycat.sqlEngine.ast.expr.numberExpr.DoubleExpr;
import io.mycat.sqlEngine.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DoubleAddExpr implements DoubleExpr {

  private final RootSessionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Double getValue() {
    Double leftValue = (Double) this.left.getValue();
    if (leftValue==null){
      return null;
    }
    Double rightValue = (Double) this.right.getValue();
    if (rightValue==null){
      return null;
    }
    return leftValue + rightValue;
  }
}
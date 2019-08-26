package cn.lightfish.sql.ast.expr.arithmeticExpr.doubleOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.expr.numberExpr.DoubleExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DoubleModExpr implements DoubleExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Double getValue() {
    Double leftValue = (Double) this.left.getValue();
    if (leftValue == null) {
      return leftValue;
    }
    Double rightValue = (Double) this.right.getValue();
    if (rightValue == null) {
      return null;
    }
    return leftValue % rightValue;
  }
}
package cn.lightfish.sql.ast.arithmeticExpr.doubleOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.DoubleExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DoubleMultipyExpr implements DoubleExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Double getValue() {
    Double leftValue = (Double) left.getValue();
    if (leftValue == null) {
      return null;
    }
    Double rightValue = (Double) right.getValue();
    if (rightValue == null) {
      return null;
    }
    return leftValue * rightValue;
  }
}
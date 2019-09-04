package cn.lightfish.sqlEngine.ast.expr.arithmeticExpr.doubleOperator;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.ast.expr.numberExpr.DoubleExpr;
import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DoubleIntegerDivsionExpr implements DoubleExpr {

  private final RootSessionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Double getValue() {
    Double leftValue = (Double) this.left.getValue();
    if (leftValue == null) {
      return null;
    }
    Double rightValue = (Double) this.right.getValue();
    if (rightValue == null) {
      return null;
    }
    return BigDecimal.valueOf(leftValue).divideToIntegralValue(BigDecimal.valueOf(rightValue))
        .doubleValue();
  }
}
package cn.lightfish.sqlEngine.ast.expr.arithmeticExpr.bigDecimalOperator;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.ast.expr.numberExpr.BigDecimalExpr;
import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigDecimalMultipyExpr implements BigDecimalExpr {

  private final RootSessionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public BigDecimal getValue() {
    BigDecimal leftValue = (BigDecimal) left.getValue();
    if (leftValue == null) {
      return null;
    }
    BigDecimal rightValue = (BigDecimal) right.getValue();
    if (rightValue == null) {
      return null;
    }
    return leftValue.multiply(rightValue);
  }
}
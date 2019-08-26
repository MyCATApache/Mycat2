package cn.lightfish.sql.ast.expr.arithmeticExpr.bigDecimalOperator;

import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.ast.expr.numberExpr.BigDecimalExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;
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
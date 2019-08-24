package cn.lightfish.sql.ast.arithmeticExpr.bigDecimalOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.BigDecimalExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigDecimalMultipyExpr implements BigDecimalExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public BigDecimal getValue() {
    BigDecimal leftValue = (BigDecimal) left.getValue();
    BigDecimal rightValue = (BigDecimal) right.getValue();
    return leftValue.multiply(rightValue);
  }
}
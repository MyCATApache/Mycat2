package cn.lightfish.sql.ast.arithmeticExpr.bigDecimalOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.BigDecimalExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigDecimalModExpr implements BigDecimalExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public BigDecimal getValue() {
    BigDecimal left = (BigDecimal) this.left.getValue();
    BigDecimal rightValue = (BigDecimal) this.right.getValue();
    return rightValue.divideAndRemainder(left)[0];
  }
}
package cn.lightfish.sql.ast.expr.arithmeticExpr.bigDecimalOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.expr.numberExpr.BigDecimalExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigDecimalModExpr implements BigDecimalExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public BigDecimal getValue() {
    BigDecimal leftValue = (BigDecimal) this.left.getValue();
    if (leftValue == null){
      return null;
    }
    BigDecimal rightValue = (BigDecimal) this.right.getValue();
    if (rightValue == null){
      return null;
    }
    return leftValue.divideAndRemainder(leftValue)[0];
  }
}
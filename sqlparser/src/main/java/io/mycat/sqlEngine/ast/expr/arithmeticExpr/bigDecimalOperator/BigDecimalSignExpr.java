package io.mycat.sqlEngine.ast.expr.arithmeticExpr.bigDecimalOperator;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.ast.expr.numberExpr.BigDecimalExpr;
import io.mycat.sqlEngine.context.RootSessionContext;
import lombok.AllArgsConstructor;

import java.math.BigDecimal;

@AllArgsConstructor
public class BigDecimalSignExpr implements BigDecimalExpr {

  private final RootSessionContext context;
  private final ValueExpr valueExpr;

  @Override
  public BigDecimal getValue() {
    BigDecimal value = (BigDecimal) valueExpr.getValue();
    if (value == null){
      return null;
    }
    return value.negate();
  }
}
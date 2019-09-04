package cn.lightfish.sqlEngine.ast.expr.arithmeticExpr.bigDecimalOperator;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.ast.expr.numberExpr.BigDecimalExpr;
import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;

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
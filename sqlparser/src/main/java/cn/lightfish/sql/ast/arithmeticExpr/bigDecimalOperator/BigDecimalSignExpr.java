package cn.lightfish.sql.ast.arithmeticExpr.bigDecimalOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.BigDecimalExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BigDecimalSignExpr implements BigDecimalExpr {

  private final RootExecutionContext context;
  private final ValueExpr valueExpr;

  @Override
  public BigDecimal getValue() {
    BigDecimal value = (BigDecimal) valueExpr.getValue();
    return value.negate();
  }
}
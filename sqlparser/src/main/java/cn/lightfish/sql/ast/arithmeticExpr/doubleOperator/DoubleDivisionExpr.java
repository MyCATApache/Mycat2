package cn.lightfish.sql.ast.arithmeticExpr.doubleOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.BigDecimalExpr;
import cn.lightfish.sql.ast.numberExpr.DoubleExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.math.BigDecimal;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DoubleDivisionExpr implements DoubleExpr {
  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Double getValue() {
    Double left = (Double) this.left.getValue();
    Double rightValue = (Double) this.right.getValue();
    return left/(rightValue);
  }
}
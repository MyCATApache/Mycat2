package cn.lightfish.sql.ast.arithmeticExpr.doubleOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.DoubleExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DoubleSubtractExpr implements DoubleExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Double getValue() {
    Double left = (Double) this.left.getValue();
    Double rightValue = (Double)this.right.getValue();
    return left+rightValue;
  }
}
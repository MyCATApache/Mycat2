package cn.lightfish.sqlEngine.ast.expr.arithmeticExpr.longOperator;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.ast.expr.numberExpr.DoubleExpr;
import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LongDivisionExpr implements DoubleExpr {

  private final RootSessionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Double getValue() {
    Long leftValue = (Long) left.getValue();
    if (leftValue == null) {
      return null;
    }
    Long rightValue = (Long) right.getValue();
    if (rightValue == null) {
      return null;
    }
    return leftValue * 1.0 / rightValue;
  }
}
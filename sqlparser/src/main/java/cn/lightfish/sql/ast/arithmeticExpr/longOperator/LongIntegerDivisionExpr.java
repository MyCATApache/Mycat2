package cn.lightfish.sql.ast.arithmeticExpr.longOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.LongExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LongIntegerDivisionExpr implements LongExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Long getValue() {
    Long leftValue = (Long) left.getValue();
    if (leftValue == null) {
      return null;
    }
    Long rightValue = (Long) right.getValue();
    if (rightValue == null) {
      return null;
    }
    return leftValue / rightValue;
  }
}
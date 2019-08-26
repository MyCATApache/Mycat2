package cn.lightfish.sql.ast.expr.booleanExpr.compareExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanGreaterThanExpr implements BooleanExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Boolean test()  {
    Comparable leftValue = left.getValue();
    Comparable rightValue = right.getValue();
    if (leftValue != null && rightValue != null) {
      return leftValue.compareTo(rightValue) > 0;
    } else {
      return null;
    }
  }
}
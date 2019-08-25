package cn.lightfish.sql.ast.booleanExpr.logicalExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanAndExpr implements BooleanExpr {

  private final RootExecutionContext context;
  private final BooleanExpr left;
  private final BooleanExpr right;

  @Override
  public Boolean test() {
    Boolean leftValue = left.test();
    Boolean rightValue = right.test();
    if (leftValue == null) {
      return false;
    }
    if (rightValue == null) {
      return false;
    }
    return leftValue && rightValue;
  }
}
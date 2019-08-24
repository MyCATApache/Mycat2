package cn.lightfish.sql.ast.booleanExpr.logicalExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanOrExpr implements BooleanExpr {

  private final RootExecutionContext context;
  private final BooleanExpr left;
  private final BooleanExpr right;

  @Override
  public Boolean test() {
    Boolean leftValue = (Boolean) left.test();
    Boolean rightValue = (Boolean) right.test();
    if (leftValue == null) {
      return false;
    }
    if (rightValue == null) {
      return false;
    }
    return leftValue || rightValue;
  }
}
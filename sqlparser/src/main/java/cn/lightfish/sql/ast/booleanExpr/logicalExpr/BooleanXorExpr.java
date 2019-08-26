package cn.lightfish.sql.ast.booleanExpr.logicalExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanXorExpr implements BooleanExpr {

  private final RootExecutionContext context;
  private final BooleanExpr left;
  private final BooleanExpr right;

  @Override
  public Boolean test() {
    Boolean leftValue = (Boolean) left.test();
    if (leftValue == null) {
      return false;
    }
    Boolean rightValue = (Boolean) right.test();
    if (rightValue == null) {
      return false;
    }
    return Boolean.logicalXor(leftValue,rightValue);
  }
}
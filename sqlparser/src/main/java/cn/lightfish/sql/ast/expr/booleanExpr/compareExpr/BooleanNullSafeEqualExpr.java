package cn.lightfish.sql.ast.expr.booleanExpr.compareExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;
import java.util.Objects;

public class BooleanNullSafeEqualExpr implements BooleanExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  public BooleanNullSafeEqualExpr(RootExecutionContext context, ValueExpr left,
      ValueExpr right) {
    this.context = context;
    this.left = left;
    this.right = right;
  }

  @Override
  public Boolean test() {
    Object leftValue = left.getValue();
    Object rightValue = right.getValue();
    if (leftValue == null && rightValue == null) {
      return true;
    } else if (leftValue != null && rightValue != null) {
      return Objects.equals(leftValue, rightValue);
    } else {
      return false;
    }
  }
}
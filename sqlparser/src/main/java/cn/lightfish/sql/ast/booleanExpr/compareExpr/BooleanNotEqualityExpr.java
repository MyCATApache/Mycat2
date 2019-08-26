package cn.lightfish.sql.ast.booleanExpr.compareExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.util.Objects;

public class BooleanNotEqualityExpr implements BooleanExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  public BooleanNotEqualityExpr(RootExecutionContext context, ValueExpr left,
      ValueExpr right) {
    this.context = context;
    this.left = left;
    this.right = right;
  }

  @Override
  public Boolean test() {
    Object leftValue = left.getValue();

    if (leftValue==null){
      return null;
    }
    Object rightValue = right.getValue();
    if (rightValue==null){
      return null;
    }
    return !Objects.equals(leftValue, rightValue);
  }
}
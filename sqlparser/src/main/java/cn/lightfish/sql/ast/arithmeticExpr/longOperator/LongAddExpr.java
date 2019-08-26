package cn.lightfish.sql.ast.arithmeticExpr.longOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.LongExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LongAddExpr implements LongExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Long getValue() {
    Long left = (Long) this.left.getValue();
    if (left == null){
      return null;
    }
    Long rightValue = (Long) this.right.getValue();
    if (rightValue == null){
      return null;
    }
    return left + rightValue;
  }
}
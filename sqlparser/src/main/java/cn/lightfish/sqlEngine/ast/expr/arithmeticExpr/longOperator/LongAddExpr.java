package cn.lightfish.sqlEngine.ast.expr.arithmeticExpr.longOperator;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.ast.expr.numberExpr.LongExpr;
import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LongAddExpr implements LongExpr {

  private final RootSessionContext context;
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
package cn.lightfish.sql.ast.bitExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.LongExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BitwiseXorExpr implements LongExpr {

  private final RootExecutionContext context;
  private final ValueExpr left;
  private final ValueExpr right;

  @Override
  public Long getValue() {
    Long leftValue = (Long) this.left.getValue();
    Long rightValue = (Long) this.right.getValue();
    return leftValue ^ rightValue;
  }
}
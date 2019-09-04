package cn.lightfish.sqlEngine.ast.expr.bitExpr;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.ast.expr.numberExpr.LongExpr;
import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BitwiseInversionExpr implements LongExpr {

  private final RootSessionContext context;
  private final ValueExpr value;

  @Override
  public Long getValue() {
    Long value = (Long) this.value.getValue();
    if (value == null){
      return null;
    }
    return ~value;
  }
}
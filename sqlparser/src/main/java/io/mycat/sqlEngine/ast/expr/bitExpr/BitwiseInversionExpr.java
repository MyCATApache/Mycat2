package io.mycat.sqlEngine.ast.expr.bitExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.ast.expr.numberExpr.LongExpr;
import io.mycat.sqlEngine.context.RootSessionContext;
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
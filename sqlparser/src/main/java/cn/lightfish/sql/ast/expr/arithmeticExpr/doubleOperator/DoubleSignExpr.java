package cn.lightfish.sql.ast.expr.arithmeticExpr.doubleOperator;

import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.ast.expr.numberExpr.DoubleExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DoubleSignExpr implements DoubleExpr {

  private final RootSessionContext context;
  private final ValueExpr value;

  @Override
  public Double getValue() {
    Double value = (Double) this.value.getValue();
    if (value == null){
      return null;
    }
    return -value;
  }
}
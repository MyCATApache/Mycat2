package cn.lightfish.sqlEngine.ast.expr.arithmeticExpr.doubleOperator;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.ast.expr.numberExpr.DoubleExpr;
import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
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
package cn.lightfish.sql.ast.arithmeticExpr.doubleOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.DoubleExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DoubleSignExpr implements DoubleExpr {

  private final RootExecutionContext context;
  private final ValueExpr value;

  @Override
  public Double getValue() {
    return -(Double)value.getValue();
  }
}
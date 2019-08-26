package cn.lightfish.sql.ast.arithmeticExpr.longOperator;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.numberExpr.LongExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class LongSignExpr implements LongExpr {

  private final RootExecutionContext context;
  private final ValueExpr value;

  @Override
  public Long getValue() {
    Long value = (Long)this.value.getValue();
    if (value == null){
      return null;
    }
    return -value;
  }
}
package io.mycat.sqlEngine.ast.expr.booleanExpr.logicalExpr;

import io.mycat.sqlEngine.context.RootSessionContext;
import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanNotExpr implements BooleanExpr {

  private final RootSessionContext context;
  private final BooleanExpr value;

  @Override
  public Boolean test() {
    Boolean value = this.value.test();
    if (value == null) {
      return false;
    }
    return !value;
  }
}
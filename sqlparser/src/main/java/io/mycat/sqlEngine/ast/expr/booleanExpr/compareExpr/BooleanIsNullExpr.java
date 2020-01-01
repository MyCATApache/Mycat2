package io.mycat.sqlEngine.ast.expr.booleanExpr.compareExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import io.mycat.sqlEngine.context.RootSessionContext;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanIsNullExpr implements BooleanExpr {

  private final RootSessionContext context;
  private final ValueExpr expr;

  @Override
  public Boolean test() {
    Comparable test = this.expr.getValue();
    return test == null;
  }
}
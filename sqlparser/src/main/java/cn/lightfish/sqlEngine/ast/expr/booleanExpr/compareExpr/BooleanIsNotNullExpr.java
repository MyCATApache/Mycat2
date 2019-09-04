package cn.lightfish.sqlEngine.ast.expr.booleanExpr.compareExpr;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanIsNotNullExpr implements BooleanExpr {

  private final RootSessionContext context;
  private final ValueExpr expr;

  @Override
  public Boolean test() {
    Comparable test = this.expr.getValue();
    return test != null;
  }
}
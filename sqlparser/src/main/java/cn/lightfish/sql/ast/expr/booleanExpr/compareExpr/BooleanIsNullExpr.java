package cn.lightfish.sql.ast.expr.booleanExpr.compareExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanIsNullExpr implements BooleanExpr {

  private final RootExecutionContext context;
  private final ValueExpr expr;

  @Override
  public Boolean test() {
    Comparable test = this.expr.getValue();
    return test == null;
  }
}
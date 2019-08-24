package cn.lightfish.sql.ast.booleanExpr.compareExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanIsExpr implements BooleanExpr {

  private final RootExecutionContext context;
  private final ValueExpr expr;
  private final ValueExpr target;

  @Override
  public Boolean test() {
    Comparable test = this.expr.getValue();
    Comparable target = this.target.getValue();
    if (test == null && target == null) {
      return true;
    }
    if (target == null) {
      return false;
    }
    Boolean exprIsTrue = Boolean.FALSE;
    if (test != null) {
      if (test instanceof Number) {
        exprIsTrue = ((Number) test).intValue() != 0;
      } else {
        exprIsTrue = true;
      }
    }
    return exprIsTrue == target;
  }
}
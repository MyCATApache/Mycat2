package cn.lightfish.sql.ast.booleanExpr.compareExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.util.function.Predicate;
import java.util.regex.Pattern;


public class BooleanLikeExpr implements BooleanExpr {

  private final RootExecutionContext context;
  private final ValueExpr expr;
  private Predicate<String> pattern;

  public BooleanLikeExpr(RootExecutionContext context, ValueExpr expr,
      String pattern) {
    this.context = context;
    this.expr = expr;
    this.pattern = Pattern.compile(pattern).asPredicate();
  }

  @Override
  public Boolean test() {
    Comparable test = this.expr.getValue();
    if (test == null) {
      return null;
    }
    return pattern.test(test.toString());
  }
}
package cn.lightfish.sql.ast.expr.booleanExpr.compareExpr;

import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.expr.ValueExpr;
import java.util.function.Predicate;
import java.util.regex.Pattern;


public class BooleanLikeExpr implements BooleanExpr {

  private final RootSessionContext context;
  private final ValueExpr expr;
  private Predicate<String> pattern;

  public BooleanLikeExpr(RootSessionContext context, ValueExpr expr,
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
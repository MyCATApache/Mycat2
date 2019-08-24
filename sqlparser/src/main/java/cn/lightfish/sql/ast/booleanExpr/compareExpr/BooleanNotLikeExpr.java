package cn.lightfish.sql.ast.booleanExpr.compareExpr;

import cn.lightfish.sql.ast.RootExecutionContext;
import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import java.util.function.Predicate;
import java.util.regex.Pattern;


public class BooleanNotLikeExpr implements BooleanExpr {

  private final BooleanLikeExpr booleanLikeExpr;

  public BooleanNotLikeExpr(RootExecutionContext context, ValueExpr expr,
      String pattern) {
    booleanLikeExpr = new BooleanLikeExpr(context, expr, pattern);
  }

  @Override
  public Boolean test() {
    Boolean test = booleanLikeExpr.test();
    if (test==null){
      return null;
    }
    return !test;
  }
}
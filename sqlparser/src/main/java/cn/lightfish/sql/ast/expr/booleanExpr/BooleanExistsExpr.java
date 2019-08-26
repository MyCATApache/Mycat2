package cn.lightfish.sql.ast.expr.booleanExpr;

import cn.lightfish.sql.executor.Executor;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class BooleanExistsExpr implements BooleanExpr {

  final Executor executor;
  final boolean not;

  @Override
  public Boolean test() {
    boolean hasNext = executor.hasNext();
    return hasNext && !not || !hasNext && not;
  }
}
package io.mycat.sqlEngine.ast.expr.booleanExpr;

import io.mycat.sqlEngine.executor.logicExecutor.Executor;
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
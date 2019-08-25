package cn.lightfish.sql.ast.booleanExpr;

import cn.lightfish.sql.ast.Executor;
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
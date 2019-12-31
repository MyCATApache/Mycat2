package io.mycat.sqlEngine.executor.logicExecutor;

import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import io.mycat.sqlEngine.schema.BaseColumnDefinition;

public class FilterExecutor implements Executor {

  final Executor executor;
  final BooleanExpr filter;
  Object[] currentRow;

  public FilterExecutor(Executor executor, BooleanExpr filter) {
    this.executor = executor;
    this.filter = filter;
  }


  @Override
  public  BaseColumnDefinition[] columnDefList() {
    return executor.columnDefList();
  }

  @Override
  public boolean hasNext() {
    while (executor.hasNext()) {
      currentRow = executor.next();
      if (filter.test() != Boolean.FALSE) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Object[] next() {
    return currentRow;
  }
}
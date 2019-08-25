package cn.lightfish.sql.ast.executor;

import cn.lightfish.sql.ast.Executor;
import cn.lightfish.sql.ast.booleanExpr.BooleanExpr;
import io.mycat.schema.MycatColumnDefinition;

public class FilterExecutor implements Executor {

  final Executor executor;
  final BooleanExpr filter;
  Object[] currentRow;

  public FilterExecutor(Executor executor, BooleanExpr filter) {
    this.executor = executor;
    this.filter = filter;
  }


  @Override
  public  MycatColumnDefinition[] columnDefList() {
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
package io.mycat.sqlEngine.persistent;

import io.mycat.sqlEngine.executor.logicExecutor.Executor;
import io.mycat.sqlEngine.schema.TableColumnDefinition;

public interface QueryPersistent extends Executor {



  public default TableColumnDefinition[] columnDefList(){
    return new TableColumnDefinition[0];
  }


  public boolean hasNext();

  public Object[] next();
}
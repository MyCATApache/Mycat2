package io.mycat.sqlEngine.executor;

import io.mycat.sqlEngine.executor.logicExecutor.Executor;
import io.mycat.sqlEngine.schema.BaseColumnDefinition;

import java.util.Iterator;

public class DefExecutor implements Executor {

  private final Iterator<Object[]> iterator;
  private final BaseColumnDefinition[] columnList;

  public DefExecutor(BaseColumnDefinition[] columnList, Iterator<Object[]> iterator) {
    this.columnList = columnList;
    this.iterator = iterator;
  }
  public DefExecutor(BaseColumnDefinition[] columnList, Iterable iterable) {
    this.columnList = columnList;
    this.iterator = iterable.iterator();
  }

  @Override
  public BaseColumnDefinition[] columnDefList() {
    return columnList;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public Object[] next() {
    Object[] next = iterator.next();
    for (int i = 0; i < columnList.length; i++) {
      try {
        next[i] = columnList[i].getType().cast(next[i]);
      }catch (Exception e){
        e.printStackTrace();
      }
    }
    return next;
  }
}
package cn.lightfish.sql.executor;

import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.schema.SimpleColumnDefinition;
import java.util.Iterator;

public class DefExecutor implements Executor {

  private final Iterator<Object[]> iterator;
  private final SimpleColumnDefinition[] columnList;

  public DefExecutor(SimpleColumnDefinition[] columnList, Iterator<Object[]> iterator) {
    this.columnList = columnList;
    this.iterator = iterator;
  }
  public DefExecutor(SimpleColumnDefinition[] columnList, Iterable iterable) {
    this.columnList = columnList;
    this.iterator = iterable.iterator();
  }

  @Override
  public SimpleColumnDefinition[] columnDefList() {
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
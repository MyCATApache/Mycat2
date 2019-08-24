package cn.lightfish.sql.ast;

import io.mycat.schema.MycatColumnDefinition;
import java.util.Iterator;

public class SimpleExecutor implements Executor {

  private final Iterator<Object[]> iterator;
  private final MycatColumnDefinition[] columnList;

  public SimpleExecutor(MycatColumnDefinition[] columnList, Iterator<Object[]> iterator) {
    this.columnList = columnList;
    this.iterator = iterator;
  }

  @Override
  public MycatColumnDefinition[] columnDefList() {
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
      next[i] = columnList[i].getType().cast(next[i]);
    }
    return next;
  }
}
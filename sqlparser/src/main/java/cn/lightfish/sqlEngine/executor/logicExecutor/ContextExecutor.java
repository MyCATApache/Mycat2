package cn.lightfish.sqlEngine.executor.logicExecutor;

import cn.lightfish.sqlEngine.context.RootSessionContext;
import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;

public class ContextExecutor implements Executor {

  public  final RootSessionContext context;
  protected Executor executor;
  final int startIndex;

  public ContextExecutor(RootSessionContext context, Executor executor,int startIndex) {
    this.context = context;
    this.executor = executor;
    this.startIndex = startIndex;
  }

  @Override
  public BaseColumnDefinition[] columnDefList() {
    return executor.columnDefList();
  }

  @Override
  public boolean hasNext() {
    return this.executor.hasNext();
  }

  @Override
  public Object[] next() {
    Object[] objects = this.executor.next();
    Object[] scope = context.scope;
    System.arraycopy(objects, 0, scope, startIndex, objects.length);
    return objects;
  }
}
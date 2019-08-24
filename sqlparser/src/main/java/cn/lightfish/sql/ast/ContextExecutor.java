package cn.lightfish.sql.ast;

import io.mycat.schema.MycatColumnDefinition;

public class ContextExecutor implements Executor {

  final RootExecutionContext context;
  final Executor executor;
  final int startIndex;

  public ContextExecutor(RootExecutionContext context, Executor executor,int startIndex) {
    this.context = context;
    this.executor = executor;
    this.startIndex = startIndex;
  }

  @Override
  public MycatColumnDefinition[] columnDefList() {
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
package cn.lightfish.sqlEngine.executor.logicExecutor;

import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;

public class ProjectExecutor extends AbsractExecutor {

  private final ValueExpr[] exprs;
  final Executor executor;

  public ProjectExecutor(BaseColumnDefinition[] columnDefinitions, ValueExpr[] exprs,Executor executor) {
    super(columnDefinitions);
    this.exprs = exprs;
    this.executor = executor;
  }
  @Override
  public boolean hasNext() {
    return this.executor.hasNext();
  }

  @Override
  public Object[] next() {
    executor.next();
    Object[] res = new Object[exprs.length];
    for (int i = 0; i <exprs.length; i++) {
      res[i] = exprs[i].getValue();
    }
    return res;
  }
}
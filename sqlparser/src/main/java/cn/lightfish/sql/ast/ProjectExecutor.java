package cn.lightfish.sql.ast;

import cn.lightfish.sql.ast.valueExpr.ValueExpr;
import io.mycat.schema.MycatColumnDefinition;

public class ProjectExecutor extends AbsractExecutor {

  private final ValueExpr[] exprs;
  final Executor executor;

  public ProjectExecutor(MycatColumnDefinition[] columnDefinitions, ValueExpr[] exprs,Executor executor) {
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
    Object[] res = executor.next();
    for (int i = 0; i < res.length; i++) {
      res[i] = exprs[i].getValue();
    }
    return res;
  }
}
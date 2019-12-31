package io.mycat.sqlEngine.executor.logicExecutor;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.schema.BaseColumnDefinition;

public class LogicJoinExecutor implements Executor {

  final Executor left;
  final Executor right;
  final JoinType joinType;
  final BaseColumnDefinition[] columnDefList;
  final ValueExpr condition;

  public LogicJoinExecutor(Executor left, Executor right,
      JoinType joinType, BaseColumnDefinition[] columnDefList,
      ValueExpr condition) {
    this.left = left;
    this.right = right;
    this.joinType = joinType;
    this.columnDefList = columnDefList;
    this.condition = condition;
  }

  @Override
  public BaseColumnDefinition[] columnDefList() {
    return new BaseColumnDefinition[0];
  }

  @Override
  public boolean hasNext() {
    return false;
  }

  @Override
  public Object[] next() {
    return new Object[0];
  }
}
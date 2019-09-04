package cn.lightfish.sqlEngine.ast.complier;

import cn.lightfish.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import cn.lightfish.sqlEngine.ast.optimizer.queryCondition.ConditionCollector;
import cn.lightfish.sqlEngine.context.GlobalContext;
import cn.lightfish.sqlEngine.executor.logicExecutor.Executor;
import cn.lightfish.sqlEngine.executor.logicExecutor.ExecutorType;
import cn.lightfish.sqlEngine.executor.logicExecutor.FilterExecutor;
import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.optimizer.Optimizers;
import java.util.List;

public class RootQueryComplier {

  final ComplierContext context;
  final ExprComplier exprComplier;
  final TableSourceComplier tableSourceComplier;

  public RootQueryComplier(ComplierContext context) {
    this.context = context;
    this.exprComplier = new ExprComplier(context);
    this.tableSourceComplier = new TableSourceComplier(context);
  }

  public Executor complieRootQuery(SQLSelectStatement x) {
    SQLSelectQueryBlock rootQuery = x.getSelect().getQueryBlock();
    ProjectComplier projectComplier = context.getProjectComplier();
    ExprComplier exprComplier = context.getExprComplier();
    List<String> aliasList = projectComplier.exractColumnName(rootQuery);
    optimizeAst(x);
    createColumnAllocator(x);
    collectSubQuery(x);
    ConditionCollector conditionCollector = new ConditionCollector();
    x.accept(conditionCollector);
    Executor executor = context.createTableSource(rootQuery.getFrom(), rootQuery.getWhere(), 0, -1, ExecutorType.QUERY);
    executor = createFilter(rootQuery, executor, exprComplier);
    executor = projectComplier
        .createProject(rootQuery.getSelectList(), aliasList, executor);
    context.runtimeContext.setQueryExecutor(executor);
    return executor;
  }

  private Executor createFilter(SQLSelectQueryBlock rootQuery,
      Executor rootTableSource, ExprComplier exprComplier) {
    if (rootQuery.getWhere() != null) {
      rootTableSource = new FilterExecutor(rootTableSource,
          (BooleanExpr) exprComplier.createExpr(rootQuery.getWhere()));
    }
    return rootTableSource;
  }


  private void createColumnAllocator(SQLSelectStatement x) {
    context.createColumnAllocator(x);
  }

  private void collectSubQuery(SQLSelectStatement x) {
    context.collectSubQuery(x);
  }

  private void optimizeAst(SQLSelectStatement x) {
    Optimizers.optimize(x, DbType.mysql, GlobalContext.INSTANCE.CACHE_REPOSITORY);
  }


}
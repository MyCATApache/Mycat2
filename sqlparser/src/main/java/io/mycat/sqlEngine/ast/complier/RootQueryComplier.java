/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.sqlEngine.ast.complier;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.optimizer.Optimizers;
import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import io.mycat.sqlEngine.ast.optimizer.queryCondition.ConditionCollector;
import io.mycat.sqlEngine.context.GlobalContext;
import io.mycat.sqlEngine.executor.logicExecutor.Executor;
import io.mycat.sqlEngine.executor.logicExecutor.ExecutorType;
import io.mycat.sqlEngine.executor.logicExecutor.FilterExecutor;

import java.util.List;
/**
 * @author Junwen Chen
 **/
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
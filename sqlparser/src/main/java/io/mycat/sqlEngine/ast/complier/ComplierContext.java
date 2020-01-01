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

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import io.mycat.sqlEngine.ast.optimizer.ColumnCollector;
import io.mycat.sqlEngine.ast.optimizer.SubqueryOptimizer;
import io.mycat.sqlEngine.context.RootSessionContext;
import io.mycat.sqlEngine.executor.logicExecutor.Executor;
import io.mycat.sqlEngine.executor.logicExecutor.ExecutorType;
import io.mycat.sqlEngine.executor.logicExecutor.LogicLeafTableExecutor;

import java.util.*;
/**
 * @author Junwen Chen
 **/
public class ComplierContext {

  final RootSessionContext runtimeContext;
  private ColumnAllocator columnAllocatior;
  private Map<MySqlSelectQueryBlock, SubqueryOptimizer.CorrelatedQuery> correlateQueries;
  private List<MySqlSelectQueryBlock> normalQueries;

  private final TableSourceComplier tableSourceComplier ;
  private final RootQueryComplier rootQueryComplier ;
  private final ExprComplier exprComplier ;
  private final ProjectComplier projectComplier;
  private final SubQueryComplier subQueryComplier;


  public ComplierContext(RootSessionContext runtimeContext) {
    Objects.requireNonNull(runtimeContext);
    this.runtimeContext = runtimeContext;
    this. tableSourceComplier = new TableSourceComplier(this);
    this.rootQueryComplier = new RootQueryComplier(this);
    this. exprComplier = new ExprComplier(this);
    this.projectComplier = new ProjectComplier(this);
    this. subQueryComplier = new SubQueryComplier(this);
  }

  public void createColumnAllocator(
      SQLStatement x) {
    ColumnCollector columnCollector = new ColumnCollector(true);
    x.accept(columnCollector);
    HashMap<SQLColumnDefinition, Integer> columnIndexMap = new HashMap<>();
    HashMap<SQLTableSource, Integer> tableSourceColumnStartIndexMap = new HashMap<>();
    HashMap<SQLTableSource, List<SQLColumnDefinition>> tableSourceColumnMap = new HashMap<>();
    columnCollector.getTableSourceColumnMap().forEach((tableSource, value) -> {
      tableSourceColumnStartIndexMap.put(tableSource, columnIndexMap.size());
      List<SQLColumnDefinition> columns = new ArrayList<>(new HashSet<>(value.values()));
      tableSourceColumnMap.put(tableSource, columns);
      columns.forEach(column -> columnIndexMap.put(column, columnIndexMap.size()));
    });
    this.columnAllocatior = new ColumnAllocator(this, columnIndexMap, tableSourceColumnMap,
        tableSourceColumnStartIndexMap);
    this.runtimeContext.createTableSourceContext(this.columnAllocatior.scopeSize());
  }

  public void collectSubQuery(SQLStatement x) {
    SubqueryOptimizer subqueryCollector = new SubqueryOptimizer();
    x.accept(subqueryCollector);
    this.correlateQueries = subqueryCollector.getCorrelateQueries();
    this.normalQueries = subqueryCollector.getNormalQueries();
  }

  public boolean isNormalQuery(MySqlSelectQueryBlock queryBlock){
    return this.normalQueries.contains(queryBlock);
  }


  public Executor createTableSource(SQLTableSource tableSource, SQLExpr where,
                                    long offset, long rowCount, ExecutorType type) {
    if (tableSource == null) {
      return null;
    }
    if (tableSource instanceof SQLExprTableSource) {
      SQLExprTableSource table = (SQLExprTableSource) tableSource;
      return tableSourceComplier.createLeafTableSource(table,offset, rowCount,type);
    } else if (tableSource instanceof SQLSubqueryTableSource) {
      tableSourceComplier.createTableSource((SQLSubqueryTableSource)tableSource);
    } else if (tableSource instanceof SQLJoinTableSource) {
      tableSourceComplier.createTableSource((SQLJoinTableSource)tableSource);
    } else if (tableSource instanceof SQLUnionQueryTableSource) {
      tableSourceComplier.createTableSource((SQLUnionQueryTableSource)tableSource);
    } else if (tableSource instanceof SQLUnnestTableSource) {
      tableSourceComplier.createTableSource((SQLUnnestTableSource)tableSource);
    } else if (tableSource instanceof SQLLateralViewTableSource) {
      tableSourceComplier.createTableSource((SQLLateralViewTableSource)tableSource);
    } else if (tableSource instanceof SQLValuesTableSource) {
      tableSourceComplier.createTableSource((SQLValuesTableSource)tableSource);
    } else {
      throw new UnsupportedOperationException();
    }
    throw new UnsupportedOperationException();
  }

  public TableSourceComplier getTableSourceComplier() {
    return tableSourceComplier;
  }

  public RootQueryComplier getRootQueryComplier() {
    return rootQueryComplier;
  }

  public ExprComplier getExprComplier() {
    return exprComplier;
  }

  public ProjectComplier getProjectComplier() {
    return projectComplier;
  }

  public ColumnAllocator getColumnAllocatior() {
    return columnAllocatior;
  }

  public SubQueryComplier getSubQueryComplier() {
    return subQueryComplier;
  }

  public Map<MySqlSelectQueryBlock, SubqueryOptimizer.CorrelatedQuery> getCorrelateQueries() {
    return correlateQueries;
  }

  public void registerLeafExecutor(LogicLeafTableExecutor tableExecuter) {
    runtimeContext.leafExecutor.add(tableExecuter);
  }
}
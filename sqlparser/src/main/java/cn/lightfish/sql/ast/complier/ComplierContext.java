package cn.lightfish.sql.ast.complier;

import cn.lightfish.sql.ast.optimizer.ColumnCollector;
import cn.lightfish.sql.ast.optimizer.SubqueryOptimizer;
import cn.lightfish.sql.ast.optimizer.SubqueryOptimizer.CorrelatedQuery;
import cn.lightfish.sql.context.RootSessionContext;
import cn.lightfish.sql.executor.logicExecutor.Executor;
import cn.lightfish.sql.executor.logicExecutor.ExecutorType;
import cn.lightfish.sql.executor.logicExecutor.LogicLeafTableExecutor;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLLateralViewTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLUnnestTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.util.*;

public class ComplierContext {

  final RootSessionContext runtimeContext;
  private ColumnAllocator columnAllocatior;
  private Map<MySqlSelectQueryBlock,CorrelatedQuery> correlateQueries;
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

  public Map<MySqlSelectQueryBlock,CorrelatedQuery> getCorrelateQueries() {
    return correlateQueries;
  }

  public void registerLeafExecutor(LogicLeafTableExecutor tableExecuter) {
    runtimeContext.leafExecutor.add(tableExecuter);
  }
}
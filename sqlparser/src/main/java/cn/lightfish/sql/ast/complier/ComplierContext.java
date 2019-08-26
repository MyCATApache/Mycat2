package cn.lightfish.sql.ast.complier;

import cn.lightfish.sql.ast.optimizer.ColumnCollector;
import cn.lightfish.sql.ast.optimizer.SubqueryOptimizer;
import cn.lightfish.sql.ast.optimizer.SubqueryOptimizer.CorrelatedQuery;
import cn.lightfish.sql.context.RootSessionContext;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class ComplierContext {

  final RootSessionContext runtimeContext;
  private ColumnAllocator columnAllocatior;
  private List<CorrelatedQuery> correlateQueries;
  private List<MySqlSelectQueryBlock> normalQueries;

  private final TableSourceComplier tableSourceComplier = new TableSourceComplier(this);
  private final RootQueryComplier rootQueryComplier = new RootQueryComplier(this);
  private final ExprComplier exprComplier = new ExprComplier(this);
  private final ProjectComplier projectComplier = new ProjectComplier(this);

  public ComplierContext(RootSessionContext runtimeContext) {
    this.runtimeContext = runtimeContext;
  }

  public void createColumnAllocator(
      SQLStatement x) {
    ColumnCollector columnCollector = new ColumnCollector();
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
}
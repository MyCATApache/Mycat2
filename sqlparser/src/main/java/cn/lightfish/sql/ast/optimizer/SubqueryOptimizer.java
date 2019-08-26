package cn.lightfish.sql.ast.optimizer;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class SubqueryOptimizer extends MySqlASTVisitorAdapter {

  final List<SQLExpr> currentColumnList = new ArrayList<>();
  final LinkedList<MySqlSelectQueryBlock> stack = new LinkedList<>();
  List<CorrelatedQuery> correlateQueries;
  List<MySqlSelectQueryBlock> normalQueries;


  @Override
  public boolean visit(MySqlSelectQueryBlock x) {
    stack.push(x);
    return super.visit(x);
  }

  @Override
  public void endVisit(MySqlSelectQueryBlock x) {
    boolean correlated = false;
    for (SQLExpr sqlExpr : currentColumnList) {
      correlated = correlated || findOuterRef(sqlExpr);
    }
    MySqlSelectQueryBlock current = stack.pop();
    if (!correlated && !stack.isEmpty()) {
      addNormalQuery(current);
    }
    currentColumnList.clear();
    super.endVisit(x);
  }


  @Override
  public void endVisit(SQLIdentifierExpr x) {
    currentColumnList.add(x);
    super.endVisit(x);
  }

  @Override
  public void endVisit(SQLPropertyExpr x) {
    currentColumnList.add(x);
    super.endVisit(x);
  }

  private boolean findOuterRef(SQLExpr sqlExpr) {
    if (sqlExpr instanceof SQLIdentifierExpr) {
      return findOuterRef(sqlExpr, ((SQLIdentifierExpr) sqlExpr).getResolvedColumn(),
          ((SQLIdentifierExpr) sqlExpr).getResolvedTableSource());
    } else {
      return findOuterRef(sqlExpr, ((SQLPropertyExpr) sqlExpr).getResolvedColumn(),
          ((SQLPropertyExpr) sqlExpr).getResolvedTableSource());
    }
  }

  private boolean findOuterRef(SQLExpr x,
      SQLColumnDefinition resolvedColumn,
      SQLTableSource resolvedTableSource) {
    if (resolvedColumn != null && !stack.isEmpty()) {
      if (!resolvedTableSource.equals(stack.peek().getFrom())) {
        addCorrelatedQuery(x, resolvedTableSource);
        return true;
      }
    }
    return false;
  }

  private void addNormalQuery(MySqlSelectQueryBlock current) {
    if (normalQueries == null) {
      normalQueries = new ArrayList<>();
    }
    normalQueries.add(current);
  }

  private void addCorrelatedQuery(SQLExpr x, SQLTableSource resolvedTableSource) {
    CorrelatedQuery correlatedQuery = new CorrelatedQuery(stack.peek());
    correlatedQuery.addOutColumnRef(resolvedTableSource, x);
    if (correlateQueries == null) {
      correlateQueries = new ArrayList<>();
    }
    correlateQueries.add(correlatedQuery);
  }


  public static class CorrelatedQuery {

    final MySqlSelectQueryBlock queryBlock;
    final Map<SQLExpr, SQLTableSource> outColumn = new HashMap<>();

    CorrelatedQuery(
        MySqlSelectQueryBlock queryBlock) {
      this.queryBlock = queryBlock;
    }

    public MySqlSelectQueryBlock getQueryBlock() {
      return queryBlock;
    }

    void addOutColumnRef(SQLTableSource tableSource, SQLExpr column) {
      outColumn.put(column, tableSource);
    }

    public Map<SQLExpr, SQLTableSource> getOutColumn() {
      return outColumn;
    }
  }

  public List<CorrelatedQuery> getCorrelateQueries() {
    return correlateQueries == null ? Collections.emptyList() : correlateQueries;
  }

  public List<MySqlSelectQueryBlock> getNormalQueries() {
    return normalQueries == null ? Collections.emptyList() : normalQueries;
  }
}
package cn.lightfish.sql.ast.optimizer;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;

public class ColumnOptimizer extends MySqlASTVisitorAdapter {

  final Map<SQLTableSource, SelectColumn> currentColumnList = new HashMap<>();

  @Override
  public boolean visit(MySqlSelectQueryBlock x) {
    SQLTableSource from = x.getFrom();
    if (from != null) {
      currentColumnList.put(from, new SelectColumn());
    }
    return super.visit(x);
  }

  @Override
  public void endVisit(MySqlSelectQueryBlock x) {
    SQLTableSource from = x.getFrom();
    if (from != null) {
      SelectColumn selectColumn = currentColumnList.get(from);
      if (selectColumn != null) {
        HashSet<SQLColumnDefinition> selectSet = new HashSet<>();
        for (SQLSelectItem sqlSelectItem : x.getSelectList()) {
          SQLColumnDefinition columnDef = getColumnDef(sqlSelectItem.getExpr());
          if (columnDef != null) {
            selectSet.add(columnDef);
          }
        }
        for (Entry<SQLExpr, SQLColumnDefinition> entry : selectColumn.columnList
            .entrySet()) {
          if (!selectSet.contains(entry.getValue())) {
            SQLExpr sqlExpr = entry.getKey().clone();
            SQLSelectItem sqlSelectItem = new SQLSelectItem(sqlExpr);
            sqlExpr.setParent(sqlSelectItem);
            sqlSelectItem.setParent(x);
            x.getSelectList().add(sqlSelectItem);
          }
        }
      }
    }
    super.endVisit(x);
  }

  private SQLColumnDefinition getColumnDef(SQLExpr sqlExpr) {
    SQLColumnDefinition resolvedColumn = null;
    if (sqlExpr instanceof SQLIdentifierExpr) {
      resolvedColumn = ((SQLIdentifierExpr) sqlExpr).getResolvedColumn();
    } else if (sqlExpr instanceof SQLPropertyExpr) {
      resolvedColumn = ((SQLPropertyExpr) sqlExpr).getResolvedColumn();
    }
    return resolvedColumn;
  }


  @Override
  public void endVisit(SQLIdentifierExpr x) {
    collectColumn(x);
    super.endVisit(x);
  }

  @Override
  public void endVisit(SQLPropertyExpr x) {
    collectColumn(x);
    super.endVisit(x);
  }

  private void collectColumn(SQLExpr sqlExpr) {
    SQLColumnDefinition resolvedColumn = null;
    SQLTableSource resolvedTableSource = null;
    if (sqlExpr instanceof SQLIdentifierExpr) {
      resolvedColumn = ((SQLIdentifierExpr) sqlExpr).getResolvedColumn();
      resolvedTableSource = ((SQLIdentifierExpr) sqlExpr).getResolvedTableSource();
    } else {
      resolvedColumn = ((SQLPropertyExpr) sqlExpr).getResolvedColumn();
      resolvedTableSource = ((SQLPropertyExpr) sqlExpr).getResolvedTableSource();
    }
    SelectColumn selectColumn;
    if (resolvedTableSource != null && resolvedColumn != null) {
      selectColumn = currentColumnList.get(resolvedTableSource);
      if (selectColumn == null) {
        selectColumn = new SelectColumn();
        currentColumnList.put(resolvedTableSource, selectColumn);
      }
      selectColumn.columnList.put(sqlExpr, resolvedColumn);
    }
  }

  public static class SelectColumn {
    final Map<SQLExpr, SQLColumnDefinition> columnList = new HashMap<>();
    public Map<SQLExpr, SQLColumnDefinition> getColumnMap() {
      return columnList;
    }
  }

  public Map<SQLTableSource, SelectColumn> getDatasourceMap() {
    return currentColumnList;
  }
}
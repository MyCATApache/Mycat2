package cn.lightfish.sql.ast.optimizer;

import cn.lightfish.sql.ast.converter.Converters;
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

public class ColumnCollector extends MySqlASTVisitorAdapter {

  private final Map<SQLTableSource, Map<SQLExpr, SQLColumnDefinition>> tableSourceColumnMap = new HashMap<>();

  @Override
  public boolean visit(MySqlSelectQueryBlock x) {
    SQLTableSource from = x.getFrom();
    if (from != null) {
      tableSourceColumnMap.put(from, new HashMap<>());
    }
    return super.visit(x);
  }

  @Override
  public void endVisit(MySqlSelectQueryBlock x) {
    SQLTableSource from = x.getFrom();
    if (from != null) {
      Map<SQLExpr, SQLColumnDefinition> selectColumn = tableSourceColumnMap.get(from);
      if (selectColumn != null) {
        HashSet<SQLColumnDefinition> selectSet = new HashSet<>();
        for (SQLSelectItem sqlSelectItem : x.getSelectList()) {
          SQLColumnDefinition columnDef = Converters.getColumnDef(sqlSelectItem.getExpr());
          if (columnDef != null) {
            selectSet.add(columnDef);
          }
        }
        for (Entry<SQLExpr, SQLColumnDefinition> entry : selectColumn
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
    Map<SQLExpr, SQLColumnDefinition> selectColumn;
    if (resolvedTableSource != null && resolvedColumn != null) {
      selectColumn = tableSourceColumnMap.computeIfAbsent(resolvedTableSource, k -> new HashMap<>());
      selectColumn.put(sqlExpr, resolvedColumn);
    }
  }

  public Map<SQLTableSource, Map<SQLExpr, SQLColumnDefinition>> getTableSourceColumnMap() {
    return tableSourceColumnMap;
  }

}
package io.mycat.sqlparser.util.aliasResolver;


import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLJoinTableSource;
import com.alibaba.druid.sql.ast.statement.SQLLateralViewTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSubqueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLTableSource;
import com.alibaba.druid.sql.ast.statement.SQLUnionQueryTableSource;
import com.alibaba.druid.sql.ast.statement.SQLValuesTableSource;
import com.alibaba.druid.sql.ast.statement.SQLWithSubqueryClause;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.druid.sql.visitor.SQLASTVisitorAdapter;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TableAliasCollector extends MySqlASTVisitorAdapter {

  private final Map<String, SQLTableSource> tableSourceMap = new HashMap<>();

  public TableAliasCollector() {
  }

  public boolean visit(SQLLateralViewTableSource x) {
    String alias = x.getAlias();
    if (alias == null) {
      return false;
    }
    tableSourceMap.put(alias, x);
    return true;
  }

  public boolean visit(SQLValuesTableSource x) {
    String alias = x.getAlias();
    if (alias == null) {
      return false;
    }
    tableSourceMap.put(alias, x);
    return true;
  }

  public boolean visit(SQLUnionQueryTableSource x) {
    String alias = x.getAlias();
    if (alias == null) {
      x.getUnion().accept(this);
      return false;
    }
    tableSourceMap.put(alias, x);
    return true;
  }

  public boolean visit(SQLSubqueryTableSource x) {
    String alias = x.getAlias();
    if (alias == null) {
      x.getSelect().accept(this);
      return false;
    }
    tableSourceMap.put(alias, x);
    return true;
  }

  public boolean visit(SQLJoinTableSource x) {
    String alias = x.getAlias();
    if (alias == null) {
      return true;
    }
    tableSourceMap.put(alias, x);
    return true;
  }

  public boolean visit(SQLWithSubqueryClause.Entry x) {
    String alias = x.getAlias();
    if (alias == null) {
      return true;
    }
    tableSourceMap.put(alias, x);
    return true;
  }

  public boolean visit(SQLExprTableSource x) {
    String alias = x.getAlias();
    if (alias == null) {
      SQLExpr expr = x.getExpr();
      if (expr instanceof SQLName) {
        tableSourceMap.put(((SQLName) expr).getSimpleName(), x);
        return false;
      }
      return true;
    }

    return true;
  }

  public Collection<SQLTableSource> getTableSources() {
    return tableSourceMap.values();
  }

}
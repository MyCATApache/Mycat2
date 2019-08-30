package cn.lightfish.sqlEngine.ast.complier;

import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import cn.lightfish.sqlEngine.executor.logicExecutor.Executor;
import cn.lightfish.sqlEngine.executor.logicExecutor.OnlyProjectExecutor;
import cn.lightfish.sqlEngine.executor.logicExecutor.ProjectExecutor;
import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;
import java.util.ArrayList;
import java.util.List;

public class ProjectComplier {

  private final ComplierContext complierContext;

  public ProjectComplier(ComplierContext complierContext) {
    this.complierContext = complierContext;
  }

  public List<String> exractColumnName(SQLSelectQueryBlock rootQuery) {
    List<String> aliasList = new ArrayList<>();
    for (SQLSelectItem sqlSelectItem : rootQuery.getSelectList()) {
      aliasList.add(sqlSelectItem.toString());
    }
    return aliasList;
  }

  public Executor createProject(List<SQLSelectItem> selectItems,
      List<String> aliasList,
      Executor rootTableSource) {
    int size = aliasList != null ? aliasList.size() : selectItems.size();
    BaseColumnDefinition[] columnDefinitions = new BaseColumnDefinition[size];
    ValueExpr[] exprs = new ValueExpr[size];
    for (int i = 0; i < size; i++) {
      SQLSelectItem item = selectItems.get(i);
      exprs[i] = complierContext.getExprComplier().createExpr(item.getExpr());
      columnDefinitions[i] = new BaseColumnDefinition(
          aliasList != null ? aliasList.get(i) : item.computeAlias(), exprs[i].getType());
    }
    return createProjectExecutor(columnDefinitions, exprs, rootTableSource);
  }

  public Executor createProjectExecutor(BaseColumnDefinition[] columnDefinitions,
      ValueExpr[] exprs, Executor rootTableSource) {
    if (rootTableSource != null) {
      return new ProjectExecutor(columnDefinitions, exprs, rootTableSource);
    } else {
      Object[] res = new Object[exprs.length];
      for (int i = 0; i < exprs.length; i++) {
        res[i] = exprs[i].getValue();
      }
      return new OnlyProjectExecutor(columnDefinitions, res);
    }
  }
}
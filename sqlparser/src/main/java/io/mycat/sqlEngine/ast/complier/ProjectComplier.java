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

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.executor.logicExecutor.Executor;
import io.mycat.sqlEngine.executor.logicExecutor.OnlyProjectExecutor;
import io.mycat.sqlEngine.executor.logicExecutor.ProjectExecutor;
import io.mycat.sqlEngine.schema.BaseColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectQueryBlock;

import java.util.ArrayList;
import java.util.List;
/**
 * @author Junwen Chen
 **/
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
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
package cn.lightfish.sqlEngine.ast.complier;

import cn.lightfish.sqlEngine.ast.converter.Converters;
import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import cn.lightfish.sqlEngine.executor.logicExecutor.*;
import cn.lightfish.sqlEngine.schema.BaseColumnDefinition;
import cn.lightfish.sqlEngine.schema.DbSchemaManager;
import cn.lightfish.sqlEngine.schema.TableColumnDefinition;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLListExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.ast.statement.SQLJoinTableSource.JoinType;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Set;
/**
 * @author Junwen Chen
 **/
public class TableSourceComplier {

    private final ComplierContext complierContext;

    public TableSourceComplier(ComplierContext context) {
        this.complierContext = context;
    }

    public Executor createLeafTableSource(SQLExprTableSource tableSource, long offset, long rowCount,ExecutorType type) {
        String schema = tableSource.getSchemaObject().getSchema().getName();
        String tableName =tableSource.getSchemaObject().getName();
        ColumnAllocator columnAllocatior = complierContext.getColumnAllocatior();
        TableColumnDefinition[] columns = columnAllocatior.getLeafTableColumnDefinition(tableSource);
        LogicLeafTableExecutor tableExecuter = DbSchemaManager.INSTANCE.getLogicLeafTableSource(schema, tableName, columns, offset, rowCount,type);
        complierContext.registerLeafExecutor(tableExecuter);
        return new ContextExecutor(this.complierContext.runtimeContext, tableExecuter, columnAllocatior.getTableStartIndex(tableSource));
    }


    public void createTableSource(SQLSubqueryTableSource tableSource) {

    }

    public void createTableSource(SQLJoinTableSource tableSource) {
        SQLTableSource left = tableSource.getLeft();
        JoinType joinType = tableSource.getJoinType();
        SQLTableSource right = tableSource.getRight();
        SQLExpr condition = tableSource.getCondition();
        List<SQLExpr> using = tableSource.getUsing();

        Executor leftExecutor = complierContext.createTableSource(left, null, 0, -1,ExecutorType.QUERY);
        Executor rightExecutor = complierContext.createTableSource(left, null, 0, -1,ExecutorType.QUERY);

        while (leftExecutor.hasNext()) {
            Object[] next = leftExecutor.next();
        }
        ImmutableSet<Character> charList = ImmutableSet.of('a', 'b', 'c');
        Set<List<Character>> set = Sets.cartesianProduct(charList, charList, charList);
    }


    public void createTableSource(SQLUnionQueryTableSource tableSource) {
        SQLUnionQuery union = tableSource.getUnion();
        switch (union.getOperator()) {
            case UNION:
                break;
            case UNION_ALL:
                break;
            case MINUS:
                break;
            case EXCEPT:
                break;
            case INTERSECT:
                break;
            case DISTINCT:
                break;
        }
    }

    public void createTableSource(SQLUnnestTableSource tableSource) {

    }

    public void createTableSource(SQLLateralViewTableSource tableSource) {

    }

    public Executor createTableSource(SQLValuesTableSource tableSource) {
        ExprComplier exprComplier = complierContext.getExprComplier();
        String tableName = tableSource.getAlias2();
        List<SQLName> columns = tableSource.getColumns();
        BaseColumnDefinition[] columnDefinitions = Converters.transferColumnDefinitions(columns);
        List<SQLListExpr> values = tableSource.getValues();
        ValueExpr[][] rows= new ValueExpr[values.size()][];
        for (int i = 0; i < rows.length; i++) {
            List<SQLExpr> sqlListExpr = values.get(i).getItems();
            ValueExpr[] row= new ValueExpr[values.size()];
            for (int j = 0; j < columnDefinitions.length; j++) {
                row[j]= exprComplier.createExpr(sqlListExpr.get(j));
            }
            rows[i] = row;
        }
        return new ValuesTable(tableName,columnDefinitions,rows);
    }

}
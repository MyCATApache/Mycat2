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

import cn.lightfish.sqlEngine.ast.expr.ValueExpr;
import cn.lightfish.sqlEngine.ast.optimizer.SubqueryOptimizer;
import cn.lightfish.sqlEngine.executor.logicExecutor.Executor;
import cn.lightfish.sqlEngine.executor.logicExecutor.ExecutorType;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectItem;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.util.List;
import java.util.Map;
import java.util.Objects;
/**
 * @author Junwen Chen
 **/
public class SubQueryComplier {

    private ComplierContext complierContext;

    public SubQueryComplier(ComplierContext complierContext) {
        this.complierContext = complierContext;
    }

    public Executor createSubQuery(MySqlSelectQueryBlock queryBlock, SubQueryType type) {
        long row;
        List<SQLSelectItem> selectList = queryBlock.getSelectList();
        switch (type) {
            case TABLE:
                row = -1;
                break;
            case SCALAR:
                if (selectList.size() != 1) throw new UnsupportedOperationException();
                row = 2;
                break;
            case ROW:
                row = 2;
                break;
            case EXISTS:
                row = -1;
                break;
            case COLUMN:
                if (selectList.size() != 1) throw new UnsupportedOperationException();
                row = -1;
                break;
            default:
                throw new UnsupportedOperationException();
        }
        return complierContext.isNormalQuery(queryBlock) ?
                createNormalSubquery(queryBlock, row) :
                createCorrelateSubquery(queryBlock, row);
    }

    private Executor createCorrelateSubquery(MySqlSelectQueryBlock queryBlock, long row) {
        Map<MySqlSelectQueryBlock,SubqueryOptimizer.CorrelatedQuery> correlateQueries = Objects.requireNonNull(complierContext.getCorrelateQueries());
        SubqueryOptimizer.CorrelatedQuery correlatedQuery = correlateQueries.get(queryBlock);
        if (correlatedQuery == null) {
            List<SQLColumnDefinition> columnDefinitions = complierContext.getColumnAllocatior().getColumnDefinitionBySQLTableSource(queryBlock.getFrom());
            ExprComplier exprComplier = complierContext.getExprComplier();
            Map<SQLExpr, SQLTableSource> outColumn = correlatedQuery.getOutColumn();
            ValueExpr[] args = new ValueExpr[outColumn.size()];
            int index = 0;
            for (SQLExpr sqlExpr : outColumn.keySet()) {
                args[index] = exprComplier.createExpr(sqlExpr);
                ++index;
            }
            return null;
        } else {
            return null;
        }
    }

    private Executor createNormalSubquery(MySqlSelectQueryBlock queryBlock, long row) {
        return complierContext.getProjectComplier().createProject(queryBlock.getSelectList(), null,
                complierContext.createTableSource(queryBlock.getFrom(), queryBlock.getWhere(), 0, row, ExecutorType.QUERY));
    }
}
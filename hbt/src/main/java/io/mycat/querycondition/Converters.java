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
package io.mycat.querycondition;

import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
/**
 * @author Junwen Chen
 **/
public class Converters {
    public static SQLColumnDefinition getColumnDef(SQLExpr sqlExpr) {
        SQLColumnDefinition resolvedColumn = null;
        if (sqlExpr instanceof SQLIdentifierExpr) {
            resolvedColumn = ((SQLIdentifierExpr)sqlExpr).getResolvedColumn();
        } else if (sqlExpr instanceof SQLPropertyExpr) {
            resolvedColumn = ((SQLPropertyExpr)sqlExpr).getResolvedColumn();
        } else {
            return null;
        }
        return resolvedColumn;
    }

    public static SQLTableSource getColumnTableSource(SQLExpr rightExpr) {
        SQLTableSource tableSource = null;
        if (rightExpr instanceof SQLIdentifierExpr) {
            SQLIdentifierExpr expr = (SQLIdentifierExpr) rightExpr;
            tableSource = expr.getResolvedTableSource();
        } else if (rightExpr instanceof SQLPropertyExpr) {
            SQLPropertyExpr expr = (SQLPropertyExpr) rightExpr;
            tableSource = expr.getResolvedTableSource();
        }
        return tableSource;
    }
}
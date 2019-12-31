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
package io.mycat.sqlEngine.ast.converter;

import io.mycat.sqlEngine.ast.SQLTypeMap;
import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.ast.expr.dateExpr.DateConstExpr;
import io.mycat.sqlEngine.ast.expr.numberExpr.BigDecimalConstExpr;
import io.mycat.sqlEngine.ast.expr.numberExpr.DoubleConstExpr;
import io.mycat.sqlEngine.ast.expr.numberExpr.LongConstExpr;
import io.mycat.sqlEngine.ast.expr.stringExpr.StringConstExpr;
import io.mycat.sqlEngine.ast.expr.valueExpr.NullConstExpr;
import io.mycat.sqlEngine.schema.BaseColumnDefinition;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.fastsql.sql.ast.expr.SQLValuableExpr;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;

import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;
/**
 * @author Junwen Chen
 **/
public class Converters {
    public static BaseColumnDefinition transfor(SQLColumnDefinition value) {
        return new BaseColumnDefinition(value.getColumnName(), SQLTypeMap.toClass(value.jdbcType()));
    }

    public static ValueExpr transfor(Object value) {
        if (value == null) {
            return NullConstExpr.INSTANCE;
        }
        if (value instanceof String) {
            return new StringConstExpr(value.toString());
        } else if (value instanceof Long) {
            return new LongConstExpr((Long) value);
        } else if (value instanceof BigDecimal) {
            return new BigDecimalConstExpr((BigDecimal) value);
        } else if (value instanceof Double) {
            return new DoubleConstExpr((Double) value);
        }
        Class<?> type = value.getClass();
        if (type == java.sql.Date.class) {
            return new DateConstExpr((Date) value);
        } else if (type == java.util.Date.class) {
            return new DateConstExpr((Date) value);
        } else if (java.util.Date.class.isAssignableFrom(type)) {
            return new DateConstExpr((Date) value);
        }
        throw new UnsupportedOperationException();
    }

    public static ValueExpr transfor(SQLValuableExpr valuableExpr) {
        ConstValueExecutorConverter transfor = new ConstValueExecutorConverter();
        valuableExpr.accept(transfor);
        return transfor.getValue();
    }

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

    public static String getColumnName(SQLName sqlExpr) {
        return getColumnDef(sqlExpr).getColumnName();
    }

    public static BaseColumnDefinition[] transferColumnDefinitions(List<SQLName> columns) {
        BaseColumnDefinition[] columnDefinitions = new BaseColumnDefinition[columns.size()];
        for (int i = 0; i < columnDefinitions.length; i++) {
            SQLColumnDefinition sqlColumnDefinition = Converters.getColumnDef(columns.get(i));
            columnDefinitions[i] = Converters.transfor(sqlColumnDefinition);
        }
        return columnDefinitions;
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
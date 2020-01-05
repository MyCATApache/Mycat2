package io.mycat.queryCondition;

import com.alibaba.fastsql.sql.ast.expr.SQLBinaryOperator;
import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
/**
 * @author Junwen Chen
 **/
public class ColumnValue {
    final SQLColumnDefinition column;
    final SQLBinaryOperator operator;
    final Object value;
    final SQLTableSource tableSource;

    public ColumnValue(SQLColumnDefinition column, SQLBinaryOperator operator, Object value, SQLTableSource tableSource) {
        this.column = column;
        this.operator = operator;
        this.value = value;
        this.tableSource = tableSource;
    }


    public SQLColumnDefinition getColumn() {
        return column;
    }

    public SQLBinaryOperator getOperator() {
        return operator;
    }

    public Object getValue() {
        return value;
    }

    public SQLTableSource getTableSource() {
        return tableSource;
    }
}


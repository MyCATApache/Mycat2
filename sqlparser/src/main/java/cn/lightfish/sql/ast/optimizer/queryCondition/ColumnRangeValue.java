package cn.lightfish.sql.ast.optimizer.queryCondition;

import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;

public class ColumnRangeValue {
    final SQLColumnDefinition column;
    final Object begin;
    final Object end;
    final SQLTableSource tableSource;

    public ColumnRangeValue(SQLColumnDefinition column, Object begin, Object end, SQLTableSource tableSource) {
        this.column = column;
        this.begin = begin;
        this.end = end;
        this.tableSource = tableSource;
    }

}
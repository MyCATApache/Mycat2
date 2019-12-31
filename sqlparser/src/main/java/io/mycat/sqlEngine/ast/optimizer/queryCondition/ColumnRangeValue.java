package io.mycat.sqlEngine.ast.optimizer.queryCondition;

import com.alibaba.fastsql.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.fastsql.sql.ast.statement.SQLTableSource;
/**
 * @author Junwen Chen
 **/
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

    public SQLColumnDefinition getColumn() {
        return column;
    }


    public Object getBegin() {
        return begin;
    }


    public Object getEnd() {
        return end;
    }

    public SQLTableSource getTableSource() {
        return tableSource;
    }
}
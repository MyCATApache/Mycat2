package cn.lightfish.sqlEngine.ast.optimizer.queryCondition;

import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Junwen Chen
 **/
public class QueryDataRange {
    final List<ColumnValue> equalValues = new ArrayList<>(1);
    final List<ColumnRangeValue> rangeValues = new ArrayList<>(1);
    private MySqlSelectQueryBlock queryBlock;
    final List<QueryDataRange> children = new ArrayList<>(1);
    final List<String> messageList = new ArrayList<>(1);
    final List<ColumnValue> joinEqualValues = new ArrayList<>(1);
    final List<ColumnRangeValue> joinEangeValues = new ArrayList<>(1);
    private SQLExprTableSource tableSource;

    public QueryDataRange(MySqlSelectQueryBlock queryBlock) {
        this.queryBlock = queryBlock;
    }
    public QueryDataRange(SQLExprTableSource tableSource) {
    this.tableSource =tableSource;
    }


    public List<ColumnValue> getEqualValues() {
        return equalValues;
    }


    public List<ColumnRangeValue> getRangeValues() {
        return rangeValues;
    }

    public MySqlSelectQueryBlock getQueryBlock() {
        return queryBlock;
    }

    public List<QueryDataRange> getChildren() {
        return children;
    }

    public List<String> getMessageList() {
        return messageList;
    }

    public List<ColumnValue> getJoinEqualValues() {
        return joinEqualValues;
    }

    public List<ColumnRangeValue> getJoinEangeValues() {
        return joinEangeValues;
    }

    public SQLExprTableSource getTableSource() {
        return tableSource;
    }
}
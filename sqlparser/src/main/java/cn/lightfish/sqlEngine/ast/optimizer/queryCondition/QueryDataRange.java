package cn.lightfish.sqlEngine.ast.optimizer.queryCondition;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Junwen Chen
 **/
public class QueryDataRange {
    final List<ColumnValue> equalValues = new ArrayList<>();
    final List<ColumnRangeValue> rangeValues = new ArrayList<>();
    final MySqlSelectQueryBlock queryBlock;
    final List<QueryDataRange> children = new ArrayList<>();
    final List<String> messageList = new ArrayList<>();
    final List<ColumnValue> joinEqualValues = new ArrayList<>();
    final List<ColumnRangeValue> joinEangeValues = new ArrayList<>();

    public QueryDataRange(MySqlSelectQueryBlock queryBlock) {
        this.queryBlock = queryBlock;
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
}
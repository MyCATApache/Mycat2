package cn.lightfish.sqlEngine.ast.optimizer.queryCondition;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

import java.util.ArrayList;
import java.util.List;

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
    }
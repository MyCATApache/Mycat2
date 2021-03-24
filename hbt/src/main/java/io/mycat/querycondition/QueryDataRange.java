/**
 * Copyright (C) <2021>  <chen junwen>
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

import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;

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
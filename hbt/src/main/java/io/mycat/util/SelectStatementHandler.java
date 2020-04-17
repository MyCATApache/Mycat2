package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;

public interface SelectStatementHandler {
    public void handleSelect(SQLSelectStatement statement, Receiver receiver);
}
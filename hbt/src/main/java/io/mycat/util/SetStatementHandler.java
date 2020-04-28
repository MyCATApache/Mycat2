package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLSetStatement;

public interface SetStatementHandler {
    public void handleSet(SQLSetStatement statement, Response receiver);
}
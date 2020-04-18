package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLTruncateStatement;

public interface TruncateStatementHandler {
    void handleTruncate(SQLTruncateStatement statement, Receiver receiver);
}

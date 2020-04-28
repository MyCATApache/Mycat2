package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLReplaceStatement;

public interface ReplaceStatementHandler {
    void handleReplace(SQLReplaceStatement statement, Response receiver);
}

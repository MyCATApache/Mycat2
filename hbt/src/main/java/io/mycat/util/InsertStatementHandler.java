package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLReplaceStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;

public interface InsertStatementHandler {
    public void handleInsert(MySqlInsertStatement statement, Receiver receiver);

    public void handleReplace(SQLReplaceStatement statement, Receiver receiver);
}
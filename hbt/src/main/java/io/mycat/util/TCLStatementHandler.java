package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLCommitStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;

public interface TCLStatementHandler {
    public void handleSQLStartTransaction(SQLStartTransactionStatement statement, Receiver receiver) ;

    public void handleRollback(SQLRollbackStatement statement, Receiver receiver);

    public void handleCommit(SQLCommitStatement statement, Receiver receiver);

    public void handleSetTransaction(MySqlSetTransactionStatement statement, Receiver receiver);
}
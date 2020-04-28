package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLCommitStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLRollbackStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLStartTransactionStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSetTransactionStatement;

public interface TCLStatementHandler {
    public void handleSQLStartTransaction(SQLStartTransactionStatement statement, Response receiver) ;

    public void handleRollback(SQLRollbackStatement statement, Response receiver);

    public void handleCommit(SQLCommitStatement statement, Response receiver);

    public void handleSetTransaction(MySqlSetTransactionStatement statement, Response receiver);
}
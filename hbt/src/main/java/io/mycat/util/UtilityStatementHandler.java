package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLShowDatabasesStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLUseStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlKillStatement;

public interface UtilityStatementHandler {
    public void handleExplain(MySqlExplainStatement statement, Response receiver);

    public void handleKill(MySqlKillStatement statement, Response receiver);

    public void handleUse(SQLUseStatement statement, Response receiver);

    void handleSQLShowDatabasesStatement(SQLShowDatabasesStatement statement, Response receiver);
}
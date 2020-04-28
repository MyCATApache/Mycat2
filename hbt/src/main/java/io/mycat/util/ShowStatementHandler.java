package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowIndexesStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.*;

public interface ShowStatementHandler {
    public void handleMySqlShowCharacterSet(MySqlShowCharacterSetStatement statement, Response receiver);

    public void handleMySqlShowEngines(MySqlShowEnginesStatement statement, Response receiver);

    public void handleMySqlShowCollation(MySqlShowCollationStatement statement, Response receiver);

    public void handleMySqlShowCreateTable(SQLShowCreateTableStatement statement, Response receiver);

    public void handleMySqlShowDatabaseStatus(MySqlShowDatabaseStatusStatement statement, Response receiver);

    public void handleMySqlShowErrors(MySqlShowErrorsStatement statement, Response receiver);

    public void handleMySqlShowColumns(SQLShowColumnsStatement statement, Response receiver);

    public void handleShowIndexes(SQLShowIndexesStatement statement, Response receiver);

    public void handleMySqlShowProcessList(MySqlShowProcessListStatement statement, Response receiver);

    public void handleMySqlShowWarnings(MySqlShowWarningsStatement statement, Response receiver);


    public void handleMySqlShowVariants(MySqlShowVariantsStatement statement, Response receiver);

    public void handleShowTableStatus(MySqlShowTableStatusStatement statement, Response receiver);

    public void handleShowTables(SQLShowTablesStatement statement, Response receiver);
}
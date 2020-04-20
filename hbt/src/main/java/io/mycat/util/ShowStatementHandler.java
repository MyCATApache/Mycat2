package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowIndexesStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.*;

public interface ShowStatementHandler {
    public void handleMySqlShowCharacterSet(MySqlShowCharacterSetStatement statement, Receiver receiver);

    public void handleMySqlShowEngines(MySqlShowEnginesStatement statement, Receiver receiver);

    public void handleMySqlShowCollation(MySqlShowCollationStatement statement, Receiver receiver);

    public void handleMySqlShowCreateTable(SQLShowCreateTableStatement statement, Receiver receiver);

    public void handleMySqlShowDatabaseStatus(MySqlShowDatabaseStatusStatement statement, Receiver receiver);

    public void handleMySqlShowErrors(MySqlShowErrorsStatement statement, Receiver receiver);

    public void handleMySqlShowColumns(SQLShowColumnsStatement statement, Receiver receiver);

    public void handleShowIndexes(SQLShowIndexesStatement statement, Receiver receiver);

    public void handleMySqlShowProcessList(MySqlShowProcessListStatement statement, Receiver receiver);

    public void handleMySqlShowWarnings(MySqlShowWarningsStatement statement, Receiver receiver);


    public void handleMySqlShowVariants(MySqlShowVariantsStatement statement, Receiver receiver);

    public void handleShowTableStatus(MySqlShowTableStatusStatement statement, Receiver receiver);

    public void handleShowTables(SQLShowTablesStatement statement, Receiver receiver);
}
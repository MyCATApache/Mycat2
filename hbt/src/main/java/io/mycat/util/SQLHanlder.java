package io.mycat.util;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.*;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SQLHanlder implements SQLDispatcher {
   final SQLContext context;

    @Override
    public void handleSQLShowDatabasesStatement(SQLShowDatabasesStatement statement, Receiver receiver) {
        context.utilityStatementHandler().handleSQLShowDatabasesStatement(statement,receiver);
    }

    @Override
    public void handleMySqlHintStatement(MySqlHintStatement statement1, Receiver receiver) {
        context.hintStatementHanlder().handlehintStatement(statement1,receiver);
    }

    @Override
    public SQLStatement preHandle(SQLStatement statement) {
        statement.accept(new ContextExecuter(context));
        return statement;
    }

    @Override
    public void handleMySqlShowCharacterSet(MySqlShowCharacterSetStatement statement, Receiver receiver) {
        context.showStatementHandler().handleMySqlShowCharacterSet(statement, receiver);
    }

    @Override
    public void handleMySqlShowEngines(MySqlShowEnginesStatement statement, Receiver receiver) {
        context.showStatementHandler().handleMySqlShowEngines(statement, receiver);
    }

    @Override
    public void handleMySqlShowCollation(MySqlShowCollationStatement statement, Receiver receiver) {
        context.showStatementHandler().handleMySqlShowCollation(statement, receiver);
    }

    @Override
    public void handleMySqlShowCreateTable(SQLShowCreateTableStatement statement, Receiver receiver) {
        context.showStatementHandler().handleMySqlShowCreateTable(statement, receiver);
    }

    @Override
    public void handleMySqlShowDatabaseStatus(MySqlShowDatabaseStatusStatement statement, Receiver receiver) {
        context.showStatementHandler().handleMySqlShowDatabaseStatus(statement, receiver);
    }

    @Override
    public void handleMySqlShowErrors(MySqlShowErrorsStatement statement, Receiver receiver) {
        context.showStatementHandler().handleMySqlShowErrors(statement, receiver);
    }

    @Override
    public void handleMySqlShowColumns(SQLShowColumnsStatement statement, Receiver receiver) {
        context.showStatementHandler().handleMySqlShowColumns(statement, receiver);
    }

    @Override
    public void handleShowIndexes(SQLShowIndexesStatement statement, Receiver receiver) {
        context.showStatementHandler().handleShowIndexes(statement, receiver);
    }

    @Override
    public void handleMySqlShowProcessList(MySqlShowProcessListStatement statement, Receiver receiver) {
        context.showStatementHandler().handleMySqlShowProcessList(statement, receiver);
    }

    @Override
    public void handleMySqlShowWarnings(MySqlShowWarningsStatement statement, Receiver receiver) {
        context.showStatementHandler().handleMySqlShowWarnings(statement, receiver);
    }

    @Override
    public void handleMySqlShowVariants(MySqlShowVariantsStatement statement, Receiver receiver) {
        context.showStatementHandler().handleMySqlShowVariants(statement, receiver);
    }

    @Override
    public void handleShowTableStatus(MySqlShowTableStatusStatement statement, Receiver receiver) {
        context.showStatementHandler().handleShowTableStatus(statement, receiver);
    }

    @Override
    public void handleShowTables(SQLShowTablesStatement statement, Receiver receiver) {
        context.showStatementHandler().handleShowTables(statement, receiver);
    }

    @Override
    public void handleMySqlRenameTable(MySqlRenameTableStatement statement, Receiver receiver) {
        context.ddlStatementHandler().handleMySqlRenameTable(statement, receiver);
    }

    @Override
    public void handleDropViewStatement(SQLDropViewStatement statement, Receiver receiver) {
        context.ddlStatementHandler().handleDropViewStatement(statement, receiver);
    }

    @Override
    public void handleDropTableStatement(SQLDropTableStatement statement, Receiver receiver) {
        context.ddlStatementHandler().handleDropTableStatement(statement, receiver);
    }



    @Override
    public void handleDropDatabaseStatement(SQLDropDatabaseStatement statement, Receiver receiver) {
        context.ddlStatementHandler().handleDropDatabaseStatement(statement, receiver);
    }


    @Override
    public void handleCreateView(SQLCreateViewStatement statement, Receiver receiver) {
        context.ddlStatementHandler().handleCreateView(statement, receiver);
    }

    @Override
    public void handleCreateTable(SQLCreateTableStatement statement, Receiver receiver) {
        context.ddlStatementHandler().handleCreateTable(statement, receiver);
    }

    @Override
    public void handleCreateIndex(SQLCreateIndexStatement statement, Receiver receiver) {
        context.ddlStatementHandler().handleCreateIndex(statement, receiver);
    }

    @Override
    public void handleCreateDatabaseStatement(SQLCreateDatabaseStatement statement, Receiver receiver) {
        context.ddlStatementHandler().handleCreateDatabaseStatement(statement, receiver);
    }

    @Override
    public void handleAlterTable(SQLAlterTableStatement statement, Receiver receiver) {
        context.ddlStatementHandler().handleAlterTable(statement, receiver);
    }

    @Override
    public void handleAlterDatabase(SQLAlterDatabaseStatement statement, Receiver receiver) {
        context.ddlStatementHandler().handleAlterDatabase(statement, receiver);
    }


    @Override
    public void handleExplain(MySqlExplainStatement statement, Receiver receiver) {
        context.utilityStatementHandler().handleExplain(statement, receiver);
    }

    @Override
    public void handleKill(com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlKillStatement statement, Receiver receiver) {
        context.utilityStatementHandler().handleKill(statement, receiver);
    }

    @Override
    public void handleSQLStartTransaction(SQLStartTransactionStatement statement, Receiver receiver) {
        context.tclStatementHandler().handleSQLStartTransaction(statement, receiver);
    }

    @Override
    public void handleRollback(SQLRollbackStatement statement, Receiver receiver) {
        context.tclStatementHandler().handleRollback(statement, receiver);
    }

    @Override
    public void handleCommit(SQLCommitStatement statement, Receiver receiver) {
        context.tclStatementHandler().handleCommit(statement, receiver);
    }

    @Override
    public void handleUse(SQLUseStatement statement, Receiver receiver) {
        context.utilityStatementHandler().handleUse(statement, receiver);
    }

    @Override
    public void handleSetTransaction(MySqlSetTransactionStatement statement, Receiver receiver) {
        context.tclStatementHandler().handleSetTransaction(statement, receiver);
    }

    @Override
    public void handleSet(SQLSetStatement statement, Receiver receiver) {
        context.setStatementHandler().handleSet(statement, receiver);
    }

    @Override
    public void handleTruncate(SQLTruncateStatement statement, Receiver receiver) {
        context.truncateStatementHandler().handleTruncate(statement, receiver);
    }

    @Override
    public void handleLoaddata(com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement statement, Receiver receiver) {
        context.loaddataStatementHandler().handleLoaddata(statement, receiver);
    }

    @Override
    public void handleUpdate(MySqlUpdateStatement statement, Receiver receiver) {
        context.updateStatementHandler().handleUpdate(statement, receiver);
    }

    @Override
    public void handleDelete(MySqlDeleteStatement statement, Receiver receiver) {
        context.deleteStatementHandler().handleDelete(statement, receiver);
    }

    @Override
    public void handleReplace(SQLReplaceStatement statement, Receiver receiver) {
        context.replaceStatementHandler().handleReplace(statement, receiver);
    }

    @Override
    public void handleInsert(MySqlInsertStatement statement, Receiver receiver) {
        context.insertStatementHandler().handleInsert(statement, receiver);
    }

    @Override
    public void handleSelect(SQLSelectStatement statement, Receiver receiver) {
        context.selectStatementHandler().handleSelect(statement, receiver);
    }
}
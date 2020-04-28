package io.mycat.util;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.*;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SQLHanlder implements SQLDispatcher {
   final SQLContext context;

    @Override
    public void handleSQLShowDatabasesStatement(SQLShowDatabasesStatement statement, Response receiver) {
        context.utilityStatementHandler().handleSQLShowDatabasesStatement(statement,receiver);
    }

    @Override
    public void handleMySqlHintStatement(MySqlHintStatement statement1, Response receiver) {
        context.hintStatementHanlder().handlehintStatement(statement1,receiver);
    }

    @Override
    public SQLStatement preHandle(SQLStatement statement) {
        statement.accept(new ContextExecuter(context));
        return statement;
    }

    @Override
    public void handleMySqlShowCharacterSet(MySqlShowCharacterSetStatement statement, Response receiver) {
        context.showStatementHandler().handleMySqlShowCharacterSet(statement, receiver);
    }

    @Override
    public void handleMySqlShowEngines(MySqlShowEnginesStatement statement, Response receiver) {
        context.showStatementHandler().handleMySqlShowEngines(statement, receiver);
    }

    @Override
    public void handleMySqlShowCollation(MySqlShowCollationStatement statement, Response receiver) {
        context.showStatementHandler().handleMySqlShowCollation(statement, receiver);
    }

    @Override
    public void handleMySqlShowCreateTable(SQLShowCreateTableStatement statement, Response receiver) {
        context.showStatementHandler().handleMySqlShowCreateTable(statement, receiver);
    }

    @Override
    public void handleMySqlShowDatabaseStatus(MySqlShowDatabaseStatusStatement statement, Response receiver) {
        context.showStatementHandler().handleMySqlShowDatabaseStatus(statement, receiver);
    }

    @Override
    public void handleMySqlShowErrors(MySqlShowErrorsStatement statement, Response receiver) {
        context.showStatementHandler().handleMySqlShowErrors(statement, receiver);
    }

    @Override
    public void handleMySqlShowColumns(SQLShowColumnsStatement statement, Response receiver) {
        context.showStatementHandler().handleMySqlShowColumns(statement, receiver);
    }

    @Override
    public void handleShowIndexes(SQLShowIndexesStatement statement, Response receiver) {
        context.showStatementHandler().handleShowIndexes(statement, receiver);
    }

    @Override
    public void handleMySqlShowProcessList(MySqlShowProcessListStatement statement, Response receiver) {
        context.showStatementHandler().handleMySqlShowProcessList(statement, receiver);
    }

    @Override
    public void handleMySqlShowWarnings(MySqlShowWarningsStatement statement, Response receiver) {
        context.showStatementHandler().handleMySqlShowWarnings(statement, receiver);
    }

    @Override
    public void handleMySqlShowVariants(MySqlShowVariantsStatement statement, Response receiver) {
        context.showStatementHandler().handleMySqlShowVariants(statement, receiver);
    }

    @Override
    public void handleShowTableStatus(MySqlShowTableStatusStatement statement, Response receiver) {
        context.showStatementHandler().handleShowTableStatus(statement, receiver);
    }

    @Override
    public void handleShowTables(SQLShowTablesStatement statement, Response receiver) {
        context.showStatementHandler().handleShowTables(statement, receiver);
    }

    @Override
    public void handleMySqlRenameTable(MySqlRenameTableStatement statement, Response receiver) {
        context.ddlStatementHandler().handleMySqlRenameTable(statement, receiver);
    }

    @Override
    public void handleDropViewStatement(SQLDropViewStatement statement, Response receiver) {
        context.ddlStatementHandler().handleDropViewStatement(statement, receiver);
    }

    @Override
    public void handleDropTableStatement(SQLDropTableStatement statement, Response receiver) {
        context.ddlStatementHandler().handleDropTableStatement(statement, receiver);
    }



    @Override
    public void handleDropDatabaseStatement(SQLDropDatabaseStatement statement, Response receiver) {
        context.ddlStatementHandler().handleDropDatabaseStatement(statement, receiver);
    }


    @Override
    public void handleCreateView(SQLCreateViewStatement statement, Response receiver) {
        context.ddlStatementHandler().handleCreateView(statement, receiver);
    }

    @Override
    public void handleCreateTable(SQLCreateTableStatement statement, Response receiver) {
        context.ddlStatementHandler().handleCreateTable(statement, receiver);
    }

    @Override
    public void handleCreateIndex(SQLCreateIndexStatement statement, Response receiver) {
        context.ddlStatementHandler().handleCreateIndex(statement, receiver);
    }

    @Override
    public void handleCreateDatabaseStatement(SQLCreateDatabaseStatement statement, Response receiver) {
        context.ddlStatementHandler().handleCreateDatabaseStatement(statement, receiver);
    }

    @Override
    public void handleAlterTable(SQLAlterTableStatement statement, Response receiver) {
        context.ddlStatementHandler().handleAlterTable(statement, receiver);
    }

    @Override
    public void handleAlterDatabase(SQLAlterDatabaseStatement statement, Response receiver) {
        context.ddlStatementHandler().handleAlterDatabase(statement, receiver);
    }


    @Override
    public void handleExplain(MySqlExplainStatement statement, Response receiver) {
        context.utilityStatementHandler().handleExplain(statement, receiver);
    }

    @Override
    public void handleKill(com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlKillStatement statement, Response receiver) {
        context.utilityStatementHandler().handleKill(statement, receiver);
    }

    @Override
    public void handleSQLStartTransaction(SQLStartTransactionStatement statement, Response receiver) {
        context.tclStatementHandler().handleSQLStartTransaction(statement, receiver);
    }

    @Override
    public void handleRollback(SQLRollbackStatement statement, Response receiver) {
        context.tclStatementHandler().handleRollback(statement, receiver);
    }

    @Override
    public void handleCommit(SQLCommitStatement statement, Response receiver) {
        context.tclStatementHandler().handleCommit(statement, receiver);
    }

    @Override
    public void handleUse(SQLUseStatement statement, Response receiver) {
        context.utilityStatementHandler().handleUse(statement, receiver);
    }

    @Override
    public void handleSetTransaction(MySqlSetTransactionStatement statement, Response receiver) {
        context.tclStatementHandler().handleSetTransaction(statement, receiver);
    }

    @Override
    public void handleSet(SQLSetStatement statement, Response receiver) {
        context.setStatementHandler().handleSet(statement, receiver);
    }

    @Override
    public void handleTruncate(SQLTruncateStatement statement, Response receiver) {
        context.truncateStatementHandler().handleTruncate(statement, receiver);
    }

    @Override
    public void handleLoaddata(com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement statement, Response receiver) {
        context.loaddataStatementHandler().handleLoaddata(statement, receiver);
    }

    @Override
    public void handleUpdate(MySqlUpdateStatement statement, Response receiver) {
        context.updateStatementHandler().handleUpdate(statement, receiver);
    }

    @Override
    public void handleDelete(MySqlDeleteStatement statement, Response receiver) {
        context.deleteStatementHandler().handleDelete(statement, receiver);
    }

    @Override
    public void handleReplace(SQLReplaceStatement statement, Response receiver) {
        context.replaceStatementHandler().handleReplace(statement, receiver);
    }

    @Override
    public void handleInsert(MySqlInsertStatement statement, Response receiver) {
        context.insertStatementHandler().handleInsert(statement, receiver);
    }

    @Override
    public void handleSelect(SQLSelectStatement statement, Response receiver) {
        context.selectStatementHandler().handleSelect(statement, receiver);
    }
}
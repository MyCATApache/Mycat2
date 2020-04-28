package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.SQLShowColumnsStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowCreateTableStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowIndexesStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLShowTablesStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.*;
import io.mycat.upondb.MycatDBClientMediator;

public class ShowStatementHandlerImpl implements ShowStatementHandler {
    private MycatDBClientMediator mycatDBClientMediator;

    public ShowStatementHandlerImpl(MycatDBClientMediator mycatDBClientMediator) {
        this.mycatDBClientMediator = mycatDBClientMediator;
    }

    @Override
    public void handleMySqlShowCharacterSet(MySqlShowCharacterSetStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }

    @Override
    public void handleMySqlShowEngines(MySqlShowEnginesStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }

    @Override
    public void handleMySqlShowCollation(MySqlShowCollationStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }

    @Override
    public void handleMySqlShowCreateTable(SQLShowCreateTableStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }


    @Override
    public void handleMySqlShowDatabaseStatus(MySqlShowDatabaseStatusStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }

    @Override
    public void handleMySqlShowErrors(MySqlShowErrorsStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }

    @Override
    public void handleMySqlShowColumns(SQLShowColumnsStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }


    @Override
    public void handleShowIndexes(SQLShowIndexesStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }

    @Override
    public void handleMySqlShowProcessList(MySqlShowProcessListStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }

    @Override
    public void handleMySqlShowWarnings(MySqlShowWarningsStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }

    @Override
    public void handleMySqlShowVariants(MySqlShowVariantsStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }

    @Override
    public void handleShowTableStatus(MySqlShowTableStatusStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }

    @Override
    public void handleShowTables(SQLShowTablesStatement statement, Response receiver) {
        receiver.proxyShow(statement);
    }


}
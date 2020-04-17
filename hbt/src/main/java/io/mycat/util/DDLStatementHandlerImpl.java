package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;
import io.mycat.upondb.MycatDBClientMediator;

public class DDLStatementHandlerImpl implements DDLStatementHandler {
    private MycatDBClientMediator mycatDBClientMediator;

    public DDLStatementHandlerImpl(MycatDBClientMediator mycatDBClientMediator) {
        this.mycatDBClientMediator = mycatDBClientMediator;
    }

    @Override
    public void handleMySqlRenameTable(MySqlRenameTableStatement statement, Receiver receiver) {
        receiver.proxyDDL(statement);
    }


    @Override
    public void handleDropViewStatement(SQLDropViewStatement statement, Receiver receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleDropTableStatement(SQLDropTableStatement statement, Receiver receiver) {
        receiver.proxyDDL(statement);
    }


    @Override
    public void handleDropDatabaseStatement(SQLDropDatabaseStatement statement, Receiver receiver) {
        receiver.proxyDDL(statement);
    }


    @Override
    public void handleCreateView(SQLCreateViewStatement statement, Receiver receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleCreateTable(SQLCreateTableStatement statement, Receiver receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleCreateIndex(SQLCreateIndexStatement statement, Receiver receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleCreateDatabaseStatement(SQLCreateDatabaseStatement statement, Receiver receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleAlterTable(SQLAlterTableStatement statement, Receiver receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleAlterDatabase(SQLAlterDatabaseStatement statement, Receiver receiver) {
        receiver.proxyDDL(statement);
    }
}
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
    public void handleMySqlRenameTable(MySqlRenameTableStatement statement, Response receiver) {
        receiver.proxyDDL(statement);
    }


    @Override
    public void handleDropViewStatement(SQLDropViewStatement statement, Response receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleDropTableStatement(SQLDropTableStatement statement, Response receiver) {
        receiver.proxyDDL(statement);
    }


    @Override
    public void handleDropDatabaseStatement(SQLDropDatabaseStatement statement, Response receiver) {
        receiver.proxyDDL(statement);
    }


    @Override
    public void handleCreateView(SQLCreateViewStatement statement, Response receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleCreateTable(SQLCreateTableStatement statement, Response receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleCreateIndex(SQLCreateIndexStatement statement, Response receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleCreateDatabaseStatement(SQLCreateDatabaseStatement statement, Response receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleAlterTable(SQLAlterTableStatement statement, Response receiver) {
        receiver.proxyDDL(statement);
    }

    @Override
    public void handleAlterDatabase(SQLAlterDatabaseStatement statement, Response receiver) {
        receiver.proxyDDL(statement);
    }
}
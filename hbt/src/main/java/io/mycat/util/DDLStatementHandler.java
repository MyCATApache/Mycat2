package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;

public interface DDLStatementHandler {
    public void handleMySqlRenameTable(MySqlRenameTableStatement statement, Receiver receiver);

    public void handleDropViewStatement(SQLDropViewStatement statement, Receiver receiver) ;

    public void handleDropTableStatement(SQLDropTableStatement statement, Receiver receiver);

    public void handleDropDatabaseStatement(SQLDropDatabaseStatement statement, Receiver receiver);

    public void handleCreateView(SQLCreateViewStatement statement, Receiver receiver);

    public void handleCreateTable(SQLCreateTableStatement statement, Receiver receiver);

    public void handleCreateIndex(SQLCreateIndexStatement statement, Receiver receiver);

    public void handleCreateDatabaseStatement(SQLCreateDatabaseStatement statement, Receiver receiver);

    public void handleAlterTable(SQLAlterTableStatement statement, Receiver receiver) ;
    public void handleAlterDatabase(SQLAlterDatabaseStatement statement, Receiver receiver);
}
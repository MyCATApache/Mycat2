package io.mycat.util;

import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlRenameTableStatement;

public interface DDLStatementHandler {
    public void handleMySqlRenameTable(MySqlRenameTableStatement statement, Response receiver);

    public void handleDropViewStatement(SQLDropViewStatement statement, Response receiver) ;

    public void handleDropTableStatement(SQLDropTableStatement statement, Response receiver);

    public void handleDropDatabaseStatement(SQLDropDatabaseStatement statement, Response receiver);

    public void handleCreateView(SQLCreateViewStatement statement, Response receiver);

    public void handleCreateTable(SQLCreateTableStatement statement, Response receiver);

    public void handleCreateIndex(SQLCreateIndexStatement statement, Response receiver);

    public void handleCreateDatabaseStatement(SQLCreateDatabaseStatement statement, Response receiver);

    public void handleAlterTable(SQLAlterTableStatement statement, Response receiver) ;
    public void handleAlterDatabase(SQLAlterDatabaseStatement statement, Response receiver);
}
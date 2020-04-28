package io.mycat.util;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlLoadDataInFileStatement;

public interface LoaddataStatementHandler {
    public void handleLoaddata(MySqlLoadDataInFileStatement statement, Response receiver);
}
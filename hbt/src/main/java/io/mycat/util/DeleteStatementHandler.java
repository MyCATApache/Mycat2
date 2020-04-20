package io.mycat.util;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;

public interface DeleteStatementHandler {
    public void handleDelete(MySqlDeleteStatement statement, Receiver receiver) ;
}
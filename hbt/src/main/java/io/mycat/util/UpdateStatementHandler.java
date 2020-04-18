package io.mycat.util;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;

public interface UpdateStatementHandler {
    public void handleUpdate(MySqlUpdateStatement statement, Receiver receiver) ;
}
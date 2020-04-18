package io.mycat.util;

import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlHintStatement;

public interface HintStatementHanlder {
    void handlehintStatement(MySqlHintStatement statement1, Receiver receiver);
}

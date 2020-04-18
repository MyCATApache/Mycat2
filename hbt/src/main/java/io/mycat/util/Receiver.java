package io.mycat.util;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.hbt.TextUpdateInfo;
import io.mycat.upondb.PlanRunner;

import java.util.Iterator;

public interface Receiver {

    void setHasMore(boolean more);

    void sendError(Throwable e);

    void sendOk();

    void evalSimpleSql(SQLSelectStatement evalSimpleSql);

    default void proxySelect(String defaultTargetName, SQLSelectStatement statement) {
        proxySelect(defaultTargetName, statement.toString());
    }

    void proxySelect(String defaultTargetName, String statement);

    void eval(PlanRunner plan);

    void proxyUpdate(String defaultTargetName, String proxyUpdate);

    void proxyDDL(SQLStatement statement);

    void proxyShow(SQLStatement statement);

    void multiUpdate(String string, Iterator<TextUpdateInfo> apply);

    void multiInsert(String string, Iterator<TextUpdateInfo> apply);
}
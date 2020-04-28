package io.mycat.util;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.ExplainDetail;
import io.mycat.TextUpdateInfo;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.resultset.MycatResponse;

import java.util.Iterator;

public interface Response {

    void setHasMore(boolean more);

    void sendError(Throwable e);

    void sendOk();

    void evalSimpleSql(SQLSelectStatement evalSimpleSql);

    default void proxySelect(String defaultTargetName, SQLSelectStatement statement) {
        proxySelect(defaultTargetName, statement.toString());
    }

    void proxySelect(String defaultTargetName, String statement);

    void proxyUpdate(String defaultTargetName, String proxyUpdate);

    void proxyDDL(SQLStatement statement);

    void proxyShow(SQLStatement statement);

    void multiUpdate(String string, Iterator<TextUpdateInfo> apply);

    void multiInsert(String string, Iterator<TextUpdateInfo> apply);

    void sendError(String errorMessage, int errorCode);

    void sendExplain(Class defErrorCommandClass, Object map);

    void sendResultSet(RowBaseIterator rowBaseIterator);

    void sendResponse(MycatResponse[] mycatResponses);

    void rollback();

    void begin();

    void commit();

    void execute(ExplainDetail detail);
}
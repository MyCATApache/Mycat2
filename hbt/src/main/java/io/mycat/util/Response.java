package io.mycat.util;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.ExplainDetail;
import io.mycat.TextUpdateInfo;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.resultset.MycatResponse;

import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

public interface Response {

    void setExplainMode(boolean bool);

    void setHasMore(boolean more);

    void sendError(Throwable e);

    void sendOk();

    void evalSimpleSql(SQLStatement evalSimpleSql);

    default void proxySelect(String defaultTargetName, SQLSelectStatement statement) {
        proxySelect(defaultTargetName, statement.toString());
    }

    void proxySelect(String defaultTargetName, String statement);

    void proxyUpdate(String defaultTargetName, String proxyUpdate);

    void proxyDDL(SQLStatement statement);

    void proxyShow(SQLStatement statement);

    void tryBroadcast(SQLStatement statement);

    void multiUpdate(String string, Iterator<TextUpdateInfo> apply);

    void multiInsert(String string, Iterator<TextUpdateInfo> apply);

    void sendError(String errorMessage, int errorCode);

    /**
     * @param defErrorCommandClass 可空
     * @param map
     */
    void sendExplain(Class defErrorCommandClass, Object map);

    void sendResultSet(Supplier<RowBaseIterator> rowBaseIterator, Supplier<List<String>> explainSupplier);

    default public void sendResultSet(Supplier<RowBaseIterator> rowBaseIterator) {
        sendResultSet(rowBaseIterator, () -> {
            throw new UnsupportedOperationException();
        });
    }

    void sendResponse(MycatResponse[] mycatResponses, Supplier<List<String>> explainSupplier);

    void rollback();

    void begin();

    void commit();

    void execute(ExplainDetail detail);

    void multiGlobalInsert(String string, Iterator<TextUpdateInfo> apply);

    void multiGlobalUpdate(String string, Iterator<TextUpdateInfo> apply);
    void sendBinaryResultSet(Supplier<RowBaseIterator> rowBaseIterator);
}
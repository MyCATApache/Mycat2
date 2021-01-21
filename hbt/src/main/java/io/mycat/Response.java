package io.mycat;

import io.mycat.ExplainDetail;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.core.Observable;

import java.util.function.Supplier;

public interface Response {

    void sendError(Throwable e);

//    void proxySelectToPrototype(String statement);

    void proxySelect(String defaultTargetName, String statement);

    void proxyUpdate(String defaultTargetName, String proxyUpdate);

    void proxySelectToPrototype(String statement);

    void sendError(String errorMessage, int errorCode);

    void sendResultSet(Observable<Object[]> rowIterable, MycatRowMetaData mycatRowMetaData);

    void sendResultSet(RowIterable rowIterable);

    default void sendResultSet(Supplier<RowBaseIterator> rowBaseIteratorSupplier) {
        sendResultSet(rowBaseIteratorSupplier.get());
    }

    default void sendResultSet(RowBaseIterator rowBaseIterator) {
        sendResultSet(RowIterable.create(rowBaseIterator));
    }

    void rollback();

    void begin();

    void commit();

    void execute(ExplainDetail detail);

    void sendOk();

    void sendOk(long affectedRow,long lastInsertId);

    <T> T unWrapper(Class<T> clazz);

    public enum ResultType{
        DIRECT_TEXT,
        BINARY,
        TEXT
    }
}
package io.mycat;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.RowIterable;
import io.mycat.api.collector.RowObservable;
import io.vertx.core.impl.future.PromiseInternal;

import java.util.function.Supplier;

public interface Response {

    PromiseInternal<Void> sendError(Throwable e);

    PromiseInternal<Void> proxySelect(String defaultTargetName, String statement);

    PromiseInternal<Void> proxyUpdate(String defaultTargetName, String proxyUpdate);

    PromiseInternal<Void> proxySelectToPrototype(String statement);

    PromiseInternal<Void> sendError(String errorMessage, int errorCode);

    PromiseInternal<Void> sendResultSet(RowIterable rowIterable);

    default PromiseInternal<Void> sendResultSet(Supplier<RowBaseIterator> rowBaseIteratorSupplier) {
        return sendResultSet(rowBaseIteratorSupplier.get());
    }

    default PromiseInternal<Void> sendResultSet(RowBaseIterator rowBaseIterator) {
        return sendResultSet(RowIterable.create(rowBaseIterator));
    }


    default PromiseInternal<Void> sendResultSet(RowObservable rowIterable){
        throw new UnsupportedOperationException();
    }

    PromiseInternal<Void> rollback();

    PromiseInternal<Void> begin();

    PromiseInternal<Void> commit();

    PromiseInternal<Void> execute(ExplainDetail detail);

    PromiseInternal<Void> sendOk();

    PromiseInternal<Void> sendOk(long affectedRow, long lastInsertId);

    <T> T unWrapper(Class<T> clazz);
}
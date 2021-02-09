package io.mycat;

import io.mycat.api.collector.*;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionArbiter;
import io.vertx.core.impl.future.PromiseInternal;
import org.reactivestreams.Publisher;

import java.util.function.Supplier;

public interface Response {

    PromiseInternal<Void> sendError(Throwable e);

    PromiseInternal<Void> proxySelect(String defaultTargetName, String statement);

    PromiseInternal<Void> proxyUpdate(String defaultTargetName, String proxyUpdate);

    PromiseInternal<Void> proxySelectToPrototype(String statement);

    PromiseInternal<Void> sendError(String errorMessage, int errorCode);


    default PromiseInternal<Void> sendResultSet(RowBaseIterator rowBaseIterator) {
        return sendResultSet(RowIterable.create(rowBaseIterator));
    }

    default PromiseInternal<Void> sendResultSet(RowIterable rowIterable) {
       return sendResultSet(Observable.create(emitter -> {
           try (RowBaseIterator rowBaseIterator = rowIterable.get()) {
               MycatRowMetaData metaData = rowBaseIterator.getMetaData();
               emitter.onNext(new MySQLColumnDef(metaData));
               while (rowBaseIterator.next()) {
                   emitter.onNext(new MysqlRow(rowBaseIterator.getObjects()));
               }
               emitter.onComplete();
           }catch (Throwable throwable){
               emitter.onError(throwable);
           }
       }));
    }

    /**
     * check it right
     * @param rowBaseIteratorSupper
     * @return
     */
    default PromiseInternal<Void> sendResultSet(Supplier<RowBaseIterator> rowBaseIteratorSupper) {
        return sendResultSet(rowBaseIteratorSupper.get());
    }
//
//    default PromiseInternal<Void> sendResultSet(Observable<MysqlPacket> mysqlPacketObservable) {
//        return sendResultSet(RowIterable.create(rowBaseIterator));
//    }


    default PromiseInternal<Void> sendResultSet(Observable<MysqlPayloadObject> mysqlPacketObservable) {
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
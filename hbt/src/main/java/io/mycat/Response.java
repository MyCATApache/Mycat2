/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat;

import io.mycat.api.collector.*;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.reactivex.rxjava3.core.ObservableOnSubscribe;
import io.reactivex.rxjava3.internal.subscriptions.SubscriptionArbiter;
import io.vertx.core.Future;
import io.vertx.core.impl.future.PromiseInternal;
import org.reactivestreams.Publisher;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

public interface Response {

    Future<Void> sendError(Throwable e);

    default Future<Void> proxySelect(String defaultTargetName, String statement) {
        return proxySelect(Collections.singletonList(defaultTargetName), statement);
    }

    Future<Void> proxySelect(List<String> defaultTargetName, String statement);

    default Future<Void> proxyUpdate(String defaultTargetName, String proxyUpdate) {
        return proxyUpdate(Collections.singletonList(defaultTargetName), proxyUpdate);
    }

    Future<Void> proxyUpdate(List<String> defaultTargetName, String proxyUpdate);

    Future<Void> proxySelectToPrototype(String statement);

    Future<Void> sendError(String errorMessage, int errorCode);


    default Future<Void> sendResultSet(RowBaseIterator rowBaseIterator) {
        return sendResultSet(RowIterable.create(rowBaseIterator));
    }

    default Future<Void> sendResultSet(RowIterable rowIterable) {
        return sendResultSet(Observable.create(emitter -> {
            try (RowBaseIterator rowBaseIterator = rowIterable.get()) {
                MycatRowMetaData metaData = rowBaseIterator.getMetaData();
                emitter.onNext(new MySQLColumnDef(metaData));
                while (rowBaseIterator.next()) {
                    emitter.onNext(new MysqlRow(rowBaseIterator.getObjects()));
                }
                emitter.onComplete();
            } catch (Throwable throwable) {
                emitter.onError(throwable);
            }
        }));
    }

    /**
     * check it right
     *
     * @param rowBaseIteratorSupper
     * @return
     */
    default Future<Void> sendResultSet(Supplier<RowBaseIterator> rowBaseIteratorSupper) {
        return sendResultSet(rowBaseIteratorSupper.get());
    }
//
//    default PromiseInternal<Void> sendResultSet(Observable<MysqlPacket> mysqlPacketObservable) {
//        return sendResultSet(RowIterable.create(rowBaseIterator));
//    }


    default Future<Void> sendResultSet(Observable<MysqlPayloadObject> mysqlPacketObservable) {
        throw new UnsupportedOperationException();
    }

    Future<Void> rollback();

    Future<Void> begin();

    Future<Void> commit();

    Future<Void> execute(ExplainDetail detail);

    Future<Void> sendOk();

    Future<Void> sendOk(long affectedRow);

    Future<Void> sendOk(long affectedRow, long lastInsertId);

    <T> T unWrapper(Class<T> clazz);
}

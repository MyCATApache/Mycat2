/**
 * Copyright (C) <2022>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */

package io.mycat.newquery;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Collections;
import java.util.List;

public class FutureNewMycatConnectionImpl implements NewMycatConnection {
    final NewMycatConnection mycatConnection;
    Future<Void> queryCloseFuture = Future.succeededFuture();

    public FutureNewMycatConnectionImpl(NewMycatConnection mycatConnection) {
        this.mycatConnection = mycatConnection;
    }


    @Override
    public synchronized Future<RowSet> query(String sql, List<Object> params) {
        Future<Void> preFuture = this.queryCloseFuture;
        Future<RowSet> transform = preFuture.flatMap(voidAsyncResult -> mycatConnection.query(sql, params));
        this.queryCloseFuture = transform.mapEmpty();
        return transform;
    }

    @Override
    public synchronized void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
        Future<Void> preFuture = this.queryCloseFuture;
        this.queryCloseFuture = preFuture.flatMap(unused -> {
            Promise<Void> promise = Promise.promise();
            mycatConnection.prepareQuery(sql, params, new MysqlCollector() {
                @Override
                public void onColumnDef(MycatRowMetaData mycatRowMetaData) {
                    collector.onColumnDef(mycatRowMetaData);
                }

                @Override
                public void onRow(Object[] row) {
                    collector.onRow(row);
                }

                @Override
                public void onComplete() {
                    collector.onComplete();
                    promise.tryComplete();
                }

                @Override
                public void onError(Throwable e) {
                    collector.onError(e);
                    promise.fail(e);
                }
            });
            return promise.future();
        });
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Observable<Buffer> prepareQuery(String sql, List<Object> params, int serverstatus) {
        throw new UnsupportedOperationException();
    }

    @Override
    public synchronized Future<List<Object>> call(String sql) {
        Future<Void> preFuture = this.queryCloseFuture;
        Future<List<Object>> transform = preFuture.flatMap(voidAsyncResult -> mycatConnection.call(sql));
        this.queryCloseFuture = transform.mapEmpty();
        return transform;
    }

    @Override
    public synchronized Future<SqlResult> insert(String sql, List<Object> params) {
        Future<Void> preFuture = this.queryCloseFuture;
        Future<SqlResult> transform = preFuture.flatMap(voidAsyncResult -> mycatConnection.insert(sql, params));
        this.queryCloseFuture = transform.mapEmpty();
        return transform;
    }

    @Override
    public Future<SqlResult> insert(String sql) {
        return insert(sql, Collections.emptyList());
    }

    @Override
    public Future<SqlResult> update(String sql) {
        return update(sql, Collections.emptyList());
    }

    @Override
    public synchronized Future<SqlResult> update(String sql, List<Object> params) {
        Future<Void> preFuture = this.queryCloseFuture;
        Future<SqlResult> transform = preFuture.transform(voidAsyncResult -> mycatConnection.update(sql, params));
        this.queryCloseFuture = transform.mapEmpty();
        return transform;
    }

    @Override
    public Future<Void> close() {
        return mycatConnection.close();
    }

    @Override
    public void abandonConnection() {
        mycatConnection.abandonConnection();
    }

    @Override
    public Future<Void> abandonQuery() {
        return mycatConnection.abandonQuery();
    }
}

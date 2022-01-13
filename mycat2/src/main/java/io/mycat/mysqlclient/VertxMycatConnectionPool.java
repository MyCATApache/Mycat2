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
package io.mycat.mysqlclient;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableList;
import io.mycat.PreparedStatement;
import io.mycat.beans.mycat.MycatMySQLRowMetaData;
import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.mysqlclient.decoder.ObjectArrayDecoder;
import io.mycat.newquery.MysqlCollector;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.RowSet;
import io.mycat.newquery.SqlResult;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Single;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Arrays;
import java.util.List;

public class VertxMycatConnectionPool implements NewMycatConnection {
    VertxConnection connection;
    private VertxPoolConnectionImpl vertxConnectionPool;
    Future<Void> queryCloseFuture = Future.succeededFuture();

    public VertxMycatConnectionPool(VertxConnection connection, VertxPoolConnectionImpl vertxConnectionPool) {
        this.connection = connection;
        this.vertxConnectionPool = vertxConnectionPool;
    }

    @Override
    public synchronized Future<RowSet> query(String sql, List<Object> params) {
        String psql = paramize(sql, params);
        return queryCloseFuture.flatMap(unused -> {
            Promise<RowSet> promise = Promise.promise();
            ObjectArrayDecoder objectArrayDecoder = new ObjectArrayDecoder();
            Observable<Object[]> query = connection.query(psql, objectArrayDecoder);
            onSend();
            Single<RowSet> map = query.subscribeOn(Schedulers.computation()).toList().map(objects -> {

                MycatMySQLRowMetaData mycatMySQLRowMetaData = new MycatMySQLRowMetaData(Arrays.asList(objectArrayDecoder.getColumnDefPackets()));
                return new RowSet(mycatMySQLRowMetaData, objects);
            });
            map = map.doOnSuccess(objects -> promise.tryComplete(objects));
            map = map.doOnError(objects -> promise.tryFail(objects));
            map = map.doOnTerminate(() ->   onRev());
            map.subscribe();
            return promise.future();
        });
    }

    private String paramize(String sql, List<Object> params) {
        if (sql.startsWith("be")) {
            return sql;
        }
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            int index;

            @Override
            public void endVisit(SQLVariantRefExpr x) {
                if ("?".equalsIgnoreCase(x.getName())) {
                    if (index < params.size()) {
                        Object value = params.get(index++);
                        SQLReplaceable parent = (SQLReplaceable) x.getParent();
                        parent.replace(x, PreparedStatement.fromJavaObject(value));
                    }
                }
                super.endVisit(x);
            }
        });
        sql = sqlStatement.toString();
        return sql;
    }

    @Override
    public synchronized void prepareQuery(String sqlArg, List<Object> params, MysqlCollector collector) {
        this.queryCloseFuture = this.queryCloseFuture.flatMap(event -> {
            String sql = paramize(sqlArg, params);
            ObjectArrayDecoder objectArrayDecoder = new ObjectArrayDecoder() {
                @Override
                public void onColumnEnd() {
                    super.onColumnEnd();
                    ColumnDefPacket[] columnDefPackets = super.columnDefPackets;
                    collector.onColumnDef(new MycatMySQLRowMetaData(ImmutableList.copyOf(columnDefPackets)));
                }
            };
            Promise<Void> promise = Promise.promise();
            onSend();
            Observable<Object[]> query = connection.query(sql, objectArrayDecoder);
            query.subscribeOn(Schedulers.computation()).subscribe(objects -> collector.onRow(objects),
                    throwable -> {
                        promise.tryFail(throwable);
                        collector.onError(throwable);
                    },
                    () -> {
                        onRev();
                        promise.tryComplete();
                        collector.onComplete();
                    });
            return promise.future();
        });
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<List<Object>> call(String sql) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Future<SqlResult> insert(String sql, List<Object> params) {
        onSend();
        return update(sql, params).onComplete(event -> onRev());
    }

    @Override
    public Future<SqlResult> insert(String sql) {
        onSend();
        return connection.update(sql).onComplete(event -> onRev());
    }

    @Override
    public Future<SqlResult> update(String sql) {
        onSend();
        return connection.update(sql).onComplete(event -> onRev());
    }

    @Override
    public Future<SqlResult> update(String sql, List<Object> params) {
        onSend();
        return connection.update(paramize(sql, params)).onComplete(event -> onRev());
    }

    @Override
    public Future<Void> close() {
        return queryCloseFuture
                .onSuccess(event -> vertxConnectionPool.recycle(connection))
                .onFailure(event -> vertxConnectionPool.kill(connection));
    }

    @Override
    public void abandonConnection() {
        vertxConnectionPool.kill(connection);
    }

    @Override
    public synchronized Future<Void> abandonQuery() {
        Future<Void> queryCloseFuture = this.queryCloseFuture;
        if (queryCloseFuture == null) {
            return Future.succeededFuture();
        }
        return queryCloseFuture;
    }
}

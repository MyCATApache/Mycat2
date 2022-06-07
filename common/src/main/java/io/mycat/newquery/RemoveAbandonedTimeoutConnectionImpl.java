package io.mycat.newquery;

import io.mycat.MetaClusterCurrent;
import io.mycat.beans.mycat.MycatRelDataType;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class RemoveAbandonedTimeoutConnectionImpl implements NewMycatConnection {
    final NewMycatConnection connection;
    final Long timeId;
    public RemoveAbandonedTimeoutConnectionImpl(NewMycatConnection connection) {
        this.connection = connection;

        int removeAbandonedTimeoutSecond = getRemoveAbandonedTimeoutSecond();
        if (removeAbandonedTimeoutSecond > 0) {
            Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
            long period = TimeUnit.SECONDS.toMillis(removeAbandonedTimeoutSecond);
            timeId = vertx.setPeriodic(period, id -> {
                if (connection.isClosed()) {
                    vertx.cancelTimer(id);
                } else {
                    long duration = System.currentTimeMillis() - connection.getActiveTimeStamp();
                    if (duration > period) {
//                        vertx.cancelTimer(id);
                        connection.abandonConnection();
                    }
                }
            });
        } else {
            timeId = null;
        }
    }

    public String getTargetName() {
        return connection.getTargetName();
    }


    public void query(String sql, MysqlCollector collector) {
        connection.query(sql, collector);
    }

    public Future<RowSet> query(String sql) {
        return connection.query(sql);
    }

    public Future<RowSet> query(String sql, List<Object> params) {
        return connection.query(sql, params);
    }

    public void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
        connection.prepareQuery(sql, params, collector);
    }

    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, MycatRelDataType mycatRelDataType, BufferAllocator allocator) {
        return connection.prepareQuery(sql, params, mycatRelDataType, allocator);
    }

    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator) {
        return connection.prepareQuery(sql, params, allocator);
    }

    public Observable<Buffer> prepareQuery(String sql, List<Object> params, int serverstatus) {
        return connection.prepareQuery(sql, params, serverstatus);
    }

    public Future<List<Object>> call(String sql) {
        return connection.call(sql);
    }

    public Future<SqlResult> insert(String sql, List<Object> params) {
        return connection.insert(sql, params);
    }

    public Future<SqlResult> insert(String sql) {
        return connection.insert(sql);
    }

    public Future<SqlResult> update(String sql) {
        return connection.update(sql);
    }

    public Future<SqlResult> update(String sql, List<Object> params) {
        return connection.update(sql, params);
    }

    public Future<Void> close() {
        if (timeId != null) {
            Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
            vertx.cancelTimer(timeId);
        }
        return connection.close();
    }

    public boolean isClosed() {
        return connection.isClosed();
    }

    public void onSend() {
        connection.onSend();
    }

    public void onRev() {
        connection.onRev();
    }

    public void abandonConnection() {
        if (timeId != null) {
            Vertx vertx = MetaClusterCurrent.wrapper(Vertx.class);
            vertx.cancelTimer(timeId);
        }
        connection.abandonConnection();
    }

    public Future<Void> abandonQuery() {
        return connection.abandonQuery();
    }

    public boolean isQuerying() {
        return connection.isQuerying();
    }

    public void onActiveTimestamp(long timestamp) {
        connection.onActiveTimestamp(timestamp);
    }

    public long getActiveTimeStamp() {
        return connection.getActiveTimeStamp();
    }

    public int getRemoveAbandonedTimeoutSecond() {
        return connection.getRemoveAbandonedTimeoutSecond();
    }
}

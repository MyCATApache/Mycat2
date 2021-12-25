package io.mycat.monitor;

import io.mycat.IOExecutor;
import io.mycat.MetaClusterCurrent;
import io.mycat.newquery.MysqlCollector;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.newquery.RowSet;
import io.mycat.newquery.SqlResult;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

public class ThreadMycatConnectionImplWrapper implements NewMycatConnection {
    private DatabaseInstanceEntry stat;
    final NewMycatConnection newMycatConnection;

    public ThreadMycatConnectionImplWrapper(DatabaseInstanceEntry stat, NewMycatConnection newMycatConnection) {
        this.stat = stat;
        this.newMycatConnection = newMycatConnection;
    }

    @Override
    public Future<RowSet> query(String sql, List<Object> params) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.query(sql, params).onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public void prepareQuery(String sql, List<Object> params, MysqlCollector collector) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.prepareQuery(sql, params, collector);
                promise.tryComplete();
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator) {
        return newMycatConnection.prepareQuery(sql, params,allocator);
    }

    @Override
    public Future<List<Object>> call(String sql) {
        return newMycatConnection.call(sql);
    }

    @Override
    public Future<SqlResult> insert(String sql, List<Object> params) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.insert(sql, params).onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Future<SqlResult> insert(String sql) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.insert(sql).onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Future<SqlResult> update(String sql) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.update(sql).onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Future<SqlResult> update(String sql, List<Object> params) {
        IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
        return ioExecutor.executeBlocking(promise -> {
            try {
                this.stat.plusThread();
                newMycatConnection.update(sql, params).onComplete(promise);
            } catch (Exception e) {
                promise.tryFail(e);
            } finally {
                this.stat.decThread();
            }
        });
    }

    @Override
    public Future<Void> close() {
        return newMycatConnection.close();
    }

    @Override
    public void abandonConnection() {
        newMycatConnection.abandonConnection();
    }

    @Override
    public Future<Void> abandonQuery() {
        return newMycatConnection.abandonQuery();
    }
}

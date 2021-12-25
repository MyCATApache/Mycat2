package io.mycat.newquery;

import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Collections;
import java.util.List;

public interface NewMycatConnection {
    default void query(String sql, MysqlCollector collector) {
        prepareQuery(sql, Collections.emptyList(), collector);
    }

    default Future<RowSet> query(String sql) {
        return query(sql, Collections.emptyList());
    }

    Future<RowSet> query(String sql, List<Object> params);

    void prepareQuery(String sql, List<Object> params, MysqlCollector collector);

    Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator);

    Future<List<Object>> call(String sql);

    Future<SqlResult> insert(String sql, List<Object> params);

    Future<SqlResult> insert(String sql);

    Future<SqlResult> update(String sql);

    Future<SqlResult> update(String sql, List<Object> params);

    public Future<Void> close();

    default void onSend() {

    }

    default void onRev() {

    }

    public void abandonConnection();

    public Future<Void> abandonQuery();
}

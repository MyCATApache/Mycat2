package io.mycat.newquery;

import io.mycat.beans.mycat.MycatRelDataType;
import io.reactivex.rxjava3.core.Observable;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Collections;
import java.util.List;

public interface NewMycatConnection {

    String getTargetName();


    default void query(String sql, MysqlCollector collector) {
        prepareQuery(sql, Collections.emptyList(), collector);
    }

    default Future<RowSet> query(String sql) {
        return query(sql, Collections.emptyList());
    }

    Future<RowSet> query(String sql, List<Object> params);

    void prepareQuery(String sql, List<Object> params, MysqlCollector collector);

    Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, MycatRelDataType mycatRelDataType, BufferAllocator allocator);

    Observable<VectorSchemaRoot> prepareQuery(String sql, List<Object> params, BufferAllocator allocator);

    Observable<Buffer> prepareQuery(String sql, List<Object> params, int serverstatus);

    Future<List<Object>> call(String sql);

    Future<SqlResult> insert(String sql, List<Object> params);

    Future<SqlResult> insert(String sql);

    Future<SqlResult> update(String sql);

    Future<SqlResult> update(String sql, List<Object> params);

    public Future<Void> close();

    public boolean isClosed();

    default void onSend() {

    }

    default void onRev() {

    }

    public void abandonConnection();

    public Future<Void> abandonQuery();

    public boolean isQuerying();

    public void onActiveTimestamp(long timestamp);

    public long getActiveTimeStamp();

}

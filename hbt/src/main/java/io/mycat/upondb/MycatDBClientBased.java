package io.mycat.upondb;

import io.mycat.Identical;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.MycatRowMetaData;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public interface MycatDBClientBased {

    MycatDBSharedServer getUponDBSharedServer();

    MycatDBClientBasedConfig config();

    Map<String, Object> variables();

    <T> T getCache(Identical key, String targetName, String sql, List<Object> params);
    public <T> T getCacheCountDownByIdentity(Identical key, String targetName, String sql, List<Object> params);
    void cache(Identical key, String targetName, String sql, List<Object> params, Supplier<Object> o);

    <T> T removeCache(Identical key, String targetName, String sql, List<Object> params);

    RowBaseIterator prepareQuery(String targetName, String sql, List<Object> params);

    UpdateRowIteratorResponse prepareUpdate(String targetName, String sql, List<Object> params);

    UpdateRowIteratorResponse update(String targetName, String sql);

    RowBaseIterator query(MycatRowMetaData mycatRowMetaData, String targetName, String sql);
    RowBaseIterator query(String targetName, String sql);
    RowBaseIterator queryDefaultTarget(String sql);
    UpdateRowIteratorResponse updateDefaultTarget( String sql);

    UpdateRowIteratorResponse update(String targetName, List<String> sqls);

    void begin();

    void rollback();

    void commit();

    void setTransactionIsolation(int value);

    int getTransactionIsolation();

    boolean isAutocommit();

    void setAutocommit(boolean autocommit);

    void close();

    AtomicBoolean cancelFlag();

   String resolveFinalTargetName(String targetName);
}
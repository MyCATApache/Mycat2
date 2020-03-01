package io.mycat.upondb;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIterator;
import io.mycat.calcite.Identical;
import io.mycat.calcite.metadata.LogicTable;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public interface UponDBClientBased {

    UponDBSharedServer getUponDBSharedServer();

    Map<String, Map<String, LogicTable>> config();

    Map<String, Object> variables();

    void cache(Identical key, Object rowBaseIterator);

    <T> T getCache(Identical key);

    <T> T removeCache(Identical key);

    RowBaseIterator prepareQuery(String targetName, String sql, List<Object> params);

    UpdateRowIterator prepareUpdate(String targetName, String sql, List<Object> params);

    UpdateRowIterator update(String targetName, String sql);

    RowBaseIterator query(String targetName, String sql);

    UpdateRowIterator update(String targetName, List<String> sqls);

    void begin();

    void rollback();

    void commit();

    void setTransactionIsolation(int value);

    int getTransactionIsolation();

    boolean isAutocommit();

    void setAutocommit(boolean autocommit);

    void close();

    void endOfResponse();

    AtomicBoolean cancleFlag();
}
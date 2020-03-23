package io.mycat.upondb;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;

import java.util.Iterator;
import java.util.List;

public interface MycatDBClientApi  {
    public List<String> explain(String sql);

    public PrepareObject prepare(String sql);

    public RowBaseIterator executeQuery(Long id, List<Object> params);

    public UpdateRowIteratorResponse executeUpdate(Long id, List<Object> params);

    public void closePrepare(Long id);

    public Iterator<RowBaseIterator> executeSqls(String sql);

    public UpdateRowIteratorResponse update(String sql);

    public RowBaseIterator query(String sql);

    public UpdateRowIteratorResponse loadData(String sql);

    public RowBaseIterator executeRel(String text);

    public List<String> explainRel(String text);

    String getSchema();

    void begin();

    void rollback();

    void useSchema(String normalize);

    void commit();

    void setTransactionIsolation(int value);

    int getTransactionIsolation();

    boolean isAutoCommit();

    boolean isInTransaction();

    long getMaxRow();

    void setMaxRow(long value);

    void setAutoCommit(boolean autocommit);

    void set(String target, Object value);

    Object get(String target);

    public void close();

    void recycleResource();

    int getServerStatus();
}
package io.mycat.upondb;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIterator;
import io.mycat.calcite.prepare.PrepareObject;

import java.util.Iterator;
import java.util.List;

public interface UponDBClientApi {
    public PrepareObject prepare(String sql);

    public RowBaseIterator executeQueryPrepare(Long id, List<Object> params);

    public UpdateRowIterator executeUpdatePrepare(Long id, List<Object> params);
    public void closePrepare(Long id);
    public Iterator<RowBaseIterator> executeSqls(String sql);

    public UpdateRowIterator update(String sql);

    public RowBaseIterator query(String sql);

    public UpdateRowIterator loadData(String sql);

    public RowBaseIterator executeRel(String text);

    String getSchema();

    void begin();

    void rollback();

    void useSchema(String normalize);

    void commit();

    void setTransactionIsolation(int value);

    int getTransactionIsolation();

    boolean isAutocommit();

    long getMaxRow();

    void setMaxRow(long value);

    void setAutocommit(boolean autocommit);

    void set(String target, Object value);

    Object get(String target);

    public void close();

    void endOfResponse();

}
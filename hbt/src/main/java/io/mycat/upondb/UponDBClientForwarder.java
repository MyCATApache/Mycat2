package io.mycat.upondb;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIterator;
import io.mycat.calcite.prepare.PrepareObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public abstract class UponDBClientForwarder implements UponDBContext {
    private String schema;
    private long maxRow;
    private final Map<String, Object> varibables = new HashMap<>();


    @Override
    public PrepareObject prepare(String sql) {
        return getUponDBSharedServer().prepare(sql, (UponDBContext) this);
    }

    @Override
    public RowBaseIterator executeQueryPrepare(Long id, List<Object> params) {
        return getUponDBSharedServer().execute(id, params,(UponDBContext) this);
    }

    @Override
    public UpdateRowIterator executeUpdatePrepare(Long id, List<Object> params) {
        return (UpdateRowIterator) getUponDBSharedServer().execute(id, params, (UponDBContext) this);
    }

    @Override
    public Iterator<RowBaseIterator> executeSqls(String sql) {
        return (Iterator<RowBaseIterator>) getUponDBSharedServer().executeSqls(sql, (UponDBContext) this);
    }

    @Override
    public UpdateRowIterator update(String sql) {
        return getUponDBSharedServer().update(sql, (UponDBContext) this);
    }

    @Override
    public RowBaseIterator query(String sql) {
        return getUponDBSharedServer().query(sql, (UponDBContext) this);
    }

    @Override
    public UpdateRowIterator loadData(String sql) {
        return getUponDBSharedServer().loadData(sql,(UponDBContext) this);
    }

    @Override
    public RowBaseIterator executeRel(String text) {
        return getUponDBSharedServer().executeRel(text,(UponDBContext) this);
    }

    @Override
    public String getSchema() {
        return schema;
    }

    @Override
    public void useSchema(String normalize) {
        schema = normalize;
    }

    @Override
    public long getMaxRow() {
        return maxRow;
    }

    @Override
    public void setMaxRow(long value) {
        maxRow = value;
    }

    @Override
    public void set(String target, Object value) {
        varibables.put(target, value);
    }

    @Override
    public Object get(String target) {
        Object o = varibables.get(target);
        if (o == null) {
            return getUponDBSharedServer().get(target);
        } else {
            return o;
        }
    }

    @Override
    public void closePrepare(Long id) {
        getUponDBSharedServer().closePrepare(id);
    }
}

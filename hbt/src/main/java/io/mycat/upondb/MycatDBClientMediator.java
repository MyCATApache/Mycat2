package io.mycat.upondb;

import io.mycat.api.collector.RowBaseIterator;

import java.util.Iterator;
import java.util.List;

public abstract class MycatDBClientMediator implements MycatDBContext {


    @Override
    public PrepareObject prepare(String sql) {
        return getUponDBSharedServer().prepare(sql, (MycatDBContext) this);
    }

    @Override
    public RowBaseIterator executeQuery(Long id, List<Object> params) {
        return getUponDBSharedServer().execute(id, params,(MycatDBContext) this);
    }


    @Override
    public Iterator<RowBaseIterator> executeSqls(String sql) {
        return getUponDBSharedServer().executeSqls(sql, (MycatDBContext) this);
    }

    @Override
    public RowBaseIterator query(String sql) {
        return getUponDBSharedServer().query(sql, (MycatDBContext) this);
    }


    @Override
    public RowBaseIterator executeRel(String text) {
        return getUponDBSharedServer().executeRel(text,(MycatDBContext) this);
    }


    public abstract void setVariable(String target, Object value);


    public abstract Object getVariable(String target);

    @Override
    public void closePrepare(Long id) {
        getUponDBSharedServer().closePrepare(id);
    }

    @Override
    public List<String> explain(String sql) {
        return getUponDBSharedServer().explain(sql,this);
    }


    @Override
    public List<String> explainRel(String text) {
        return getUponDBSharedServer().explainRel(text,this);
    }
}

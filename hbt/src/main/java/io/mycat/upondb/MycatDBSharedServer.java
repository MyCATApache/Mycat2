package io.mycat.upondb;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.prepare.MycatSQLPrepareObject;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public interface MycatDBSharedServer {

    PrepareObject prepare(String sql, MycatDBContext dbContext);

    RowBaseIterator execute(Long id, List<Object> params, MycatDBContext dbContext);

    Iterator<RowBaseIterator> executeSqls(String sql, MycatDBContext dbContext);

    RowBaseIterator query(String sql, MycatDBContext dbContext);

    RowBaseIterator executeRel(String text, MycatDBContext dbContext);

    List<String> explain(String sql, MycatDBContext dbContext);
    List<String> explainRel(String sql, MycatDBContext dbContext);
    void closePrepare(Long id);

    Object get(String target);

    public <T> T getComponent(Byte key, Function<Byte, T> factory);

    public <T> T replaceComponent(Byte key, Function<Byte, T> factory);
    public MycatSQLPrepareObject innerQueryPrepareObject(String sql, MycatDBContext dbContext);
}
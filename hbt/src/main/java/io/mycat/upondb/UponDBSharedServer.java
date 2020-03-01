package io.mycat.upondb;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIterator;
import io.mycat.calcite.prepare.PrepareObject;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public interface UponDBSharedServer {

    PrepareObject prepare(String sql, UponDBContext dbContext);

    RowBaseIterator execute(Long id, List<Object> params,UponDBContext dbContext);

    Iterator<RowBaseIterator> executeSqls(String sql, UponDBContext dbContext);

    RowBaseIterator query(String sql, UponDBContext dbContext);

    UpdateRowIterator update(String sql, UponDBContext dbContext);

    UpdateRowIterator loadData(String sql, UponDBContext dbContext);

    RowBaseIterator executeRel(String text, UponDBContext dbContext);

    RowBaseIterator explainSql(String text, UponDBContext dbContext);

    void closePrepare(Long id);

    Object get(String target);

    public <T> T getComponent(Byte key, Function<Byte, T> factory);

    public <T> T replaceComponent(Byte key, Function<Byte, T> factory);
}
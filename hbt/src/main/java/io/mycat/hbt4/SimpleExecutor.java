package io.mycat.hbt4;

import io.mycat.mpp.Row;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.sql.util.SqlString;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class SimpleExecutor implements Executor {
    final Iterable<Row> rows;
    private Iterator<Row> iterator;

    public SimpleExecutor(Iterable<Row> rows) {
        this.rows = rows;
    }

    @Override
    public void open() {
        this.iterator = rows.iterator();
    }

    @Override
    public Row next() {
        if (iterator.hasNext()) {
            return iterator.next();
        }
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isRewindSupported() {
        return true;
    }

    @Override
    public ExplainWriter explain(ExplainWriter writer) {
        ExplainWriter explainWriter = writer.name(this.getClass().getName())
                .into();
        writer.item("rows", Linq4j.asEnumerable(rows).toList());
        return explainWriter.ret();
    }
}
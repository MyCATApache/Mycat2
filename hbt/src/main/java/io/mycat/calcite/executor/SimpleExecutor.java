//package io.mycat.calcite.executor;
//
//import io.mycat.calcite.Executor;
//import io.mycat.calcite.ExplainWriter;
//import io.mycat.mpp.Row;
//import org.apache.calcite.linq4j.Linq4j;
//
//import java.util.Iterator;
//
//public class SimpleExecutor implements Executor {
//    final Iterable<Row> rows;
//    private Iterator<Row> iterator;
//
//    public SimpleExecutor(Iterable<Row> rows) {
//        this.rows = rows;
//    }
//
//    @Override
//    public void open() {
//        this.iterator = rows.iterator();
//    }
//
//    @Override
//    public Row next() {
//        if (iterator.hasNext()) {
//            return iterator.next();
//        }
//        return null;
//    }
//
//    @Override
//    public void close() {
//
//    }
//
//    @Override
//    public boolean isRewindSupported() {
//        return true;
//    }
//
//    @Override
//    public ExplainWriter explain(ExplainWriter writer) {
//        ExplainWriter explainWriter = writer.name(this.getClass().getName())
//                .into();
//        writer.item("rows", Linq4j.asEnumerable(rows).toList());
//        return explainWriter.ret();
//    }
//}
//package io.mycat.hbt4;
//
//import io.mycat.api.collector.RowBaseIteratorCacher;
//import io.mycat.calcite.resultset.CalciteRowMetaData;
//import io.mycat.calcite.resultset.EnumeratorRowIterator;
//import io.mycat.hbt4.executor.TempResultSetFactory;
//import io.mycat.hbt4.logical.rel.MycatInsertRel;
//import io.mycat.hbt4.logical.rel.MycatUpdateRel;
//import org.apache.calcite.linq4j.Linq4j;
//import org.apache.calcite.rel.type.RelDataType;
//
//import java.util.List;
//import java.util.Objects;
//
//public class CacheExecutorImplementor extends ExecutorImplementorImpl {
//    private final Object key;
//
//
//    public CacheExecutorImplementor(Object key, DataSourceFactory factory, TempResultSetFactory tempResultSetFactory) {
//        super(factory, tempResultSetFactory);
//        this.key = Objects.requireNonNull(key);
//    }
//
//    @Override
//    public void implementRoot(MycatRel rel, List<String> aliasList) {
//        if (rel instanceof MycatInsertRel) {
//            return;
//        }
//        if (rel instanceof MycatUpdateRel) {
//            return;
//        }
//        Executor executor = rel.implement(this);
//        RelDataType rowType = rel.getRowType();
//        EnumeratorRowIterator rowIterator = new EnumeratorRowIterator(new CalciteRowMetaData(rowType.getFieldList()),
//                Linq4j.asEnumerable(() -> executor.outputObjectIterator()).enumerator(), () -> {
//        });
//        RowBaseIteratorCacher.put(this.key, rowIterator);
//    }
//}
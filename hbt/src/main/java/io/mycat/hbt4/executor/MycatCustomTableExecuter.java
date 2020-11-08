//package io.mycat.hbt4.executor;
//
//import io.mycat.hbt4.Executor;
//import io.mycat.hbt4.logical.rel.MycatCustomTable;
//import io.mycat.metadata.CustomTableHandler;
//import io.mycat.metadata.QueryBuilder;
//import io.mycat.mpp.Row;
//import org.apache.calcite.plan.RelOptUtil;
//import org.apache.calcite.rel.type.RelDataType;
//
//import java.util.Iterator;
//
//public class MycatCustomTableExecuter implements Executor {
//
//    private Iterable<Object[]> iterable;
//    private Iterator<Object[]> iterator;
//
//    public MycatCustomTableExecuter(Iterable<Object[]> iterable) {
//        this.iterable = iterable;
//    }
//
//    public static Executor create(QueryBuilder mycatCustomTable) {
//        Executor iterable = mycatCustomTable.run();
//        return new  MycatCustomTableExecuter(iterable);
//    }
//
//    @Override
//    public void open() {
//        this.iterator = iterable.iterator();
//    }
//
//    @Override
//    public Row next() {
//        if (iterator.hasNext()){
//            return null;
//        }
//        return Row.of(iterator.next());
//    }
//
//    @Override
//    public void close() {
//
//    }
//
//    @Override
//    public boolean isRewindSupported() {
//        return false;
//    }
//}

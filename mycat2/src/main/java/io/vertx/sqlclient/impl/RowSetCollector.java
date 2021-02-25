//package io.vertx.sqlclient.impl;
//
//import io.vertx.mysqlclient.impl.codec.MysqlCollector;
//import io.vertx.sqlclient.Row;
//import io.vertx.sqlclient.RowSet;
//
//import java.util.Set;
//import java.util.function.BiConsumer;
//import java.util.function.BinaryOperator;
//import java.util.function.Function;
//import java.util.function.Supplier;
//import java.util.stream.Collector;
//
//public class RowSetCollector<ELEMENT> implements MysqlCollector<RowSet<ELEMENT>> {
//    private final Collector<Row, RowSet<ELEMENT>, RowSet<ELEMENT>> collector;
//
//    public RowSetCollector(Function<Row, ELEMENT> mapper) {
//        if(mapper == null){
//            this.collector = (Collector)RowSetImpl.COLLECTOR;
//        }else {
//            this.collector = (Collector) RowSetImpl.collector(mapper);
//        }
//    }
//
//    @Override
//    public Supplier<RowSet<ELEMENT>> supplier() {
//        return collector.supplier();
//    }
//
//    @Override
//    public BiConsumer<RowSet<ELEMENT>, Row> accumulator() {
//        return collector.accumulator();
//    }
//
//    @Override
//    public BinaryOperator<RowSet<ELEMENT>> combiner() {
//        return collector.combiner();
//    }
//
//    @Override
//    public Function<RowSet<ELEMENT>, RowSet<ELEMENT>> finisher() {
//        return collector.finisher();
//    }
//
//    @Override
//    public Set<Characteristics> characteristics() {
//        return collector.characteristics();
//    }
//}
package io.mycat.vertxmycat;

import io.vertx.core.impl.future.PromiseInternal;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlResult;
import io.vertx.sqlclient.impl.command.QueryCommandBase;

import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;

public class MyStreamMysqlCollector<R> implements StreamMysqlCollector {
    private final VertxRowSetImpl<R> objects;
    private BiConsumer<Object, Row> accumulator;
    private Object o;
    private Function<Object, Object> finisher;
    private int count;
    private Collector collectorArg;

    public MyStreamMysqlCollector() {
        this.objects = new VertxRowSetImpl<>();
    }

    @Override
    public void onColumnDefinitions(MySQLRowDesc columnDefinitions, QueryCommandBase queryCommand) {
        objects.setColumnDescriptor(columnDefinitions.columnDescriptor());
        this.o = collectorArg.supplier().get();
        this.accumulator = (BiConsumer) collectorArg.accumulator();
        this.finisher = (Function) collectorArg.finisher();
        if (collectorArg instanceof StreamMysqlCollector) {
            StreamMysqlCollector collectorArg1 = (StreamMysqlCollector) collectorArg;
            collectorArg1.onColumnDefinitions(columnDefinitions, queryCommand);
        }
    }

    @Override
    public void onRow(Row row) {
        count++;
        accumulator.accept(o, row);
    }

}
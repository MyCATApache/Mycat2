package io.vertx.mysqlclient.impl.codec;

import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.sqlclient.Row;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 流式收集器
 * @author wangzihaogithub 2020-01-23
 */
public interface StreamMysqlCollector extends MysqlCollector<Void>{


    void onColumnDefinitions(MySQLRowDesc columnDefinitions);

    void onRow(Row row);

//    default void onFinish(int serverStatusFlags,long affectedRows, long lastInsertId){
//
//    }

    @Override
    default Supplier<Void> supplier() {
        return ()-> null;
    }

    @Override
    default BiConsumer<Void, Row> accumulator() {
        return (list,o)-> onRow(o);
    }

    @Override
    default BinaryOperator<Void> combiner() {
        return (o1,o2)-> o1;
    }

    @Override
    default Function<Void, Void> finisher() {
        return o -> o;
    }

}
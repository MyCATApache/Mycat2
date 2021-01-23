package io.vertx.mysqlclient.impl.codec;

import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.command.QueryCommandBase;

import java.util.function.BiConsumer;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 流式收集器
 * @author wangzihaogithub 2020-01-23
 */
public interface StreamMysqlCollector extends MysqlCollector<Void>{

    @Override
    void onColumnDefinitions(MySQLRowDesc columnDefinitions,QueryCommandBase queryCommand);

    void onRow(Row row);

    void onFinish(int serverStatusFlags,long affectedRows, long lastInsertId);

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
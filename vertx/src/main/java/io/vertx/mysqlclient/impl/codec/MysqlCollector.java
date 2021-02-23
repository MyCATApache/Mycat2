package io.vertx.mysqlclient.impl.codec;

import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.impl.RowSetCollector;
import io.vertx.sqlclient.impl.command.QueryCommandBase;

import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collector;

/**
 * 收集器
 *
 * @param <LIST> 集合
 * @author wangzihaogithub 2020-01-23
 */
public interface MysqlCollector<LIST> extends Collector<io.vertx.sqlclient.Row, LIST, LIST> {
    RowSetCollector<Row> ROW_COLLECTOR = new RowSetCollector<>(null);

    static <ELEMENT> MysqlCollector<RowSet<ELEMENT>> map(Function<Row, ELEMENT> mapper) {
        return new RowSetCollector<>(mapper);
    }

    /**
     * 列定义信息到达 (改方法在IO线程回掉，不要阻塞。 Thread.currentThread() == eventloop-thread )
     *
     * @param columnDefinitions 列定义
     * @return 列定义信息到达后， 需要返回row解析器
     */
    default void onColumnDefinitions(MySQLRowDesc columnDefinitions) {
    }

    @Override
    default Set<Characteristics> characteristics() {
        return Collections.emptySet();
    }
}
package io.mycat.alter;

import io.mycat.ExecutorUtil;
import io.mycat.NameableExecutor;
import io.mycat.assemble.MycatTest;
import io.vertx.core.CompositeFuture;
import org.junit.Assert;
import org.junit.Test;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class DDLTest implements MycatTest {
    @Test
    public void testNormal() throws Exception {
     while (true)  {
            int count = 8;
            NameableExecutor testDDLPool = null;
            List<Connection> connections = new ArrayList<>();
            try (Connection db1 = getMySQLConnection(DB1);
            ) {
                execute(db1, "drop database if exists  db1");
                testDDLPool = ExecutorUtil.create("testDDLPool", count);
                List<String> tableNames = IntStream.range(0, count).mapToObj(i -> "db1.travelrecord_" + i).collect(Collectors.toList());


                for (int i = 0; i < count; i++) {
                    connections.add(getMySQLConnection(DB_MYCAT));
                }

                ;


                execute(connections.get(0), RESET_CONFIG);
                execute(connections.get(0), "CREATE DATABASE db1");

                List< CompletableFuture<Void>> futureList = new ArrayList<>();
                List< Throwable> throwsList  = new ArrayList<>();
                for (int i = 0; i < count; i++) {
                    String tableName = tableNames.get(i);
                    Connection connection = connections.get(i);
                    connection.setSchema("db1");
                    CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                        try {
                            execute(connection, "CREATE TABLE " +
                                    tableName +
                                    " (\n" +
                                    "  `id` bigint(20) NOT NULL KEY " +
                                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4\n");
                        } catch (Throwable e) {
                            throwsList.add(e);
                        }
                    }, testDDLPool);
                    futureList.add(future);
                }
                CompletableFuture<Void> allOf = CompletableFuture.allOf(futureList.toArray(new CompletableFuture[]{}));
                allOf.get();
                Assert.assertTrue(throwsList.isEmpty());
                List<Map<String, Object>> show_tables_from_db1 = executeQuery(db1, "show tables from db1");
                Assert.assertEquals(count,show_tables_from_db1.size());
                show_tables_from_db1 = executeQuery(connections.get(0), "show tables from db1");
                Assert.assertEquals(count,show_tables_from_db1.size());
            } finally {
                for (Connection connection : connections) {
                    connection.close();
                }
                testDDLPool.shutdown();
            }
        }

    }
}

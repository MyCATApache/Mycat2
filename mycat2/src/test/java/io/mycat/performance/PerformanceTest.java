package io.mycat.performance;

import io.vertx.core.*;
import io.vertx.mysqlclient.MySQLConnectOptions;
import io.vertx.mysqlclient.MySQLConnection;

import java.util.ArrayList;
import java.util.List;

public class PerformanceTest {

    public static void main(String[] args) throws Exception{
        MySQLConnectOptions options = new MySQLConnectOptions()
                .setPort(3306)
                .setHost("127.0.0.1")
                .setUser("root")
                .setPassword("123456");
        Vertx vertx = Vertx.vertx();
        long start = System.currentTimeMillis();
        List<Future> list = new ArrayList<>();
        long count = 60000;
        long batch = 2;
        for (int i = 0; i < batch; i++) {
            Future<Void> future = Future.future((Handler<Promise<Void>>) promise -> {
                Handler<AsyncResult<MySQLConnection>> handler = new Handler<AsyncResult<MySQLConnection>>() {
                    Future<Void> future = Future.succeededFuture();
                    long start = System.currentTimeMillis();


                    @Override
                    public void handle(AsyncResult<MySQLConnection> res) {
                        if (res.succeeded()) {
                            start = System.currentTimeMillis();
                            MySQLConnection mySQLConnection = res.result();
                            for (int i1 = 0; i1 < count; i1++) {
                                future = future.compose(unused -> mySQLConnection.query("select * from db1.travelrecord").execute().onFailure(event -> System.out.println(event)).mapEmpty());
                            }
                            future = future.onSuccess(event -> {
                                long end = System.currentTimeMillis();
                                double during = end - start;
                                System.out.println(during);
                                System.out.println("qps:" + count / during * 1000);
                            });
                            future = future.onFailure(event -> System.out.println(event));
                            future= future.onSuccess(event -> mySQLConnection.close());
                            future=future.onComplete(event -> promise.tryComplete());
                        } else {
                            System.out.println("Could not connect " + res.cause());
                        }
                    }
                };
                MySQLConnection.connect(vertx, options, handler);
            });
            list.add(future);
        }
        CompositeFuture.join(list).toCompletionStage().toCompletableFuture().get();
        long end = System.currentTimeMillis();
        double during = end - start;
        System.out.println(during);
        System.out.println("qps:" + count* batch/ during * 1000);
        System.exit(0);
    }
}

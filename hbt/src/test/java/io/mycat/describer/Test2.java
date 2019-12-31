package io.mycat.describer;

import io.vertx.core.MultiMap;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;

import java.util.concurrent.atomic.AtomicLong;

public class Test2 {
    public static void main(String[] args) {
        AtomicLong count = new AtomicLong(0);
        int batch = 5;
        Vertx vertx = Vertx.vertx();
        HttpServer httpServer = vertx.createHttpServer();
        httpServer.requestHandler(httpServerRequest -> {
            httpServerRequest.setExpectMultipart(true);
            httpServerRequest.endHandler(event -> {
                MultiMap entries = httpServerRequest.formAttributes();
                System.out.println(entries);
                long limit = count.addAndGet(5);
                httpServerRequest.response().setChunked(true).end(limit - batch + "," + limit);
            });
        }).listen(8067);
    }
}
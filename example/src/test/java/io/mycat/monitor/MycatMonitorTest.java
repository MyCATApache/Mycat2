package io.mycat.monitor;

import com.alibaba.druid.util.JdbcUtils;
import io.mycat.assemble.MycatTest;
import io.mycat.util.JsonUtil;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import lombok.SneakyThrows;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testng.Assert;

import javax.annotation.concurrent.NotThreadSafe;
import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@NotThreadSafe
@net.jcip.annotations.NotThreadSafe
public class MycatMonitorTest implements MycatTest {
    public static Vertx vertx;

    @BeforeClass
    public static void beforeClass() {
        if (vertx == null) {
            vertx = Vertx.vertx();
        }

    }

    @AfterClass
    public static void afterClass() {
        if (vertx != null) {
            vertx.close();
        }
    }

    @Test
    @SneakyThrows
    public void test() {
        Class<DatabaseInstanceEntry.DatabaseInstanceMap> tClass = DatabaseInstanceEntry.DatabaseInstanceMap.class;
        String url = MycatSQLLogMonitorImpl.SHOW_DB_MONITOR_URL;
        DatabaseInstanceEntry.DatabaseInstanceMap b = fetch(url, tClass);
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);) {
            String sql = " SHOW KEYS FROM `performance_schema`.`accounts`; ";
            for (int i = 0; i < 1000; i++) {
                JdbcUtils.execute(mySQLConnection, sql);
            }
        }
        DatabaseInstanceEntry.DatabaseInstanceMap f = fetch(url, tClass);
        Assert.assertNotEquals(b.toString(), f.toString());
        System.out.println();
    }

    @Test
    @SneakyThrows
    public void test2() {
        Class<InstanceEntry> tClass = InstanceEntry.class;
        String url = MycatSQLLogMonitorImpl.SHOW_INSTANCE_MONITOR_URL;
        InstanceEntry b = fetch(url, tClass);
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);) {
            String sql = " SHOW KEYS FROM `performance_schema`.`accounts`; ";
            for (int i = 0; i < 1000; i++) {
                JdbcUtils.execute(mySQLConnection, sql);
            }
        }
        InstanceEntry f = fetch(url, tClass);
        Assert.assertNotEquals(b.toString(), f.toString());
        System.out.println();
    }

    @Test
    @SneakyThrows
    public void test3() {
        Class<RWEntry.RWEntryMap> tClass = RWEntry.RWEntryMap.class;
        String url = MycatSQLLogMonitorImpl.SHOW_RW_MONITOR_URL;
        RWEntry.RWEntryMap b = fetch(url, tClass);
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);) {
            String sql = " SHOW KEYS FROM `performance_schema`.`accounts`; ";
            for (int i = 0; i < 1000; i++) {
                JdbcUtils.execute(mySQLConnection, sql);
            }
        }
        RWEntry.RWEntryMap f = fetch(url, tClass);
        Assert.assertNotEquals(b.toString(), f.toString());
        System.out.println();
    }

    @Test
    @SneakyThrows
    public void test4() {
        try (Connection mySQLConnection = getMySQLConnection(DB_MYCAT);
             Connection prototype = getMySQLConnection(DB1);) {
            deleteData(prototype, "mycat", "sql_log");
            List<Map<String, Object>> maps = JdbcUtils.executeQuery(mySQLConnection, "/*+mycat:getSqlTimeFilter{} */", Collections.emptyList());
            Object orginalValue = maps.get(0).get("value");
            // String sql = " select sleep(1)";
            String sql = " select * FROM `performance_schema`.`accounts`; ";
            Assert.assertEquals(0, count(prototype, "mycat", "sql_log"));

            JdbcUtils.execute(mySQLConnection, "/*+mycat:setSqlTimeFilter{value:0} */", Collections.emptyList());
            maps=  JdbcUtils.executeQuery(mySQLConnection, "/*+mycat:getSqlTimeFilter{} */", Collections.emptyList());
            Assert.assertEquals("[{value=0}]",maps.toString());
            JdbcUtils.executeQuery(mySQLConnection, sql,Collections.emptyList());
            Thread.sleep(1000);
            long count = count(prototype, "mycat", "sql_log");
            Assert.assertEquals(1,count);
            System.out.println();
        }

    }

    @SneakyThrows
    public static <T> T fetch(String url, Class<T> tClass) {
        Future<T> future = Future.future(promise -> {
            HttpClient httpClient1 = vertx.createHttpClient();
            Future<HttpClientRequest> request1 = httpClient1.request(HttpMethod.GET, 9066, "127.0.0.1", url);
            request1.onSuccess(clientRequest -> clientRequest.response(ar -> {
                if (ar.succeeded()) {
                    HttpClientResponse response = ar.result();
                    response.bodyHandler(event -> {
                        String s = event.toString();
                        T instanceEntry = JsonUtil.from(s, tClass);
                        promise.tryComplete(instanceEntry);
                    });
                } else {
                    promise.tryComplete();
                }
            }).end());
            request1.onFailure(new Handler<Throwable>() {
                @Override
                public void handle(Throwable throwable) {
                    promise.fail(throwable);
                }
            });
        });
        return future.toCompletionStage().toCompletableFuture().get();
    }
}

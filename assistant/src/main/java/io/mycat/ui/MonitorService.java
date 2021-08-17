package io.mycat.ui;

import io.mycat.monitor.DatabaseInstanceEntry;
import io.mycat.monitor.InstanceEntry;
import io.mycat.monitor.MycatSQLLogMonitorImpl;
import io.mycat.monitor.RWEntry;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.Json;
import lombok.Getter;

@Getter
public class MonitorService {
   public static final Vertx vertx = Vertx.vertx();
    String ip ;

    public MonitorService(String ip, int port) {
        this.ip = ip;
        this.port = port;
    }

    int port;


    public Future<InstanceEntry> fetchInstanceEntry() {
        return fetchData(MycatSQLLogMonitorImpl.SHOW_INSTANCE_MONITOR_URL, InstanceEntry.class);
    }

    public Future<DatabaseInstanceEntry.DatabaseInstanceMap> fetchDBEntry() {
        return fetchData(MycatSQLLogMonitorImpl.SHOW_DB_MONITOR_URL, DatabaseInstanceEntry.DatabaseInstanceMap.class);
    }

    public Future<RWEntry.RWEntryMap> fetchRWEntry() {
        return fetchData(MycatSQLLogMonitorImpl.SHOW_RW_MONITOR_URL, RWEntry.RWEntryMap.class);
    }

    public <T> Future<T> fetchData(String url, Class<T> tClass) {
        HttpClient httpClient = vertx.createHttpClient();
        return Future.future(promise -> {
            Future<HttpClientRequest> request = httpClient.request(HttpMethod.GET, port, ip, url);
            request.onSuccess(clientRequest -> clientRequest.response(ar -> {
                if (ar.succeeded()) {
                    HttpClientResponse response = ar.result();
                    response.bodyHandler(event -> {
                        String s = event.toString();
                        T instanceEntry = Json.decodeValue(s, tClass);
                        promise.tryComplete(instanceEntry);
                    });
                }
            }).end());
        });
    }

}

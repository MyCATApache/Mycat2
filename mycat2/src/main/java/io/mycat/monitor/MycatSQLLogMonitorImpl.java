package io.mycat.monitor;

import io.mycat.config.MonitorConfig;
import io.mycat.config.SqlLogConfig;
import io.mycat.config.TimerConfig;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import lombok.SneakyThrows;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class MycatSQLLogMonitorImpl extends MycatSQLLogMonitor {
    private final MonitorConfig monitorConfig;
    DatabaseInstanceEntry.DatabaseInstanceMap databaseInstanceMapSnapshot;
    InstanceEntry instanceSnapshot;
    RWEntry.RWEntryMap rwEntryMapSnapshot;
    Consumer<SqlEntry> logConsumer;

    public static final String SHOW_INSTANCE_MONITOR_URL = "/ShowInstanceMonitor";
    public static final String SHOW_DB_MONITOR_URL = "/ShowDbMonitor";
    public static final String SHOW_RW_MONITOR_URL = "/ShowRwMonitor";
    public static final String QUERY_SQL_LOG = "/QuerySqlLog";
    @SneakyThrows
    public MycatSQLLogMonitorImpl(long workerId, MonitorConfig monitorConfig, Vertx vertx) {
        super(workerId);
        this.monitorConfig = monitorConfig;


        initBaseMonitor(vertx);

        String clazz = monitorConfig.getSqlLog().getClazz();
        if (monitorConfig.getSqlLog().isOpen() && clazz != null) {
            Class<?> aClass = Class.forName(clazz);
            logConsumer = (Consumer) aClass.newInstance();
        }
        String ip = monitorConfig.getIp();
        int port = monitorConfig.getPort();
        if (ip != null) {
            HttpServer httpServer = vertx.createHttpServer();
            httpServer.requestHandler(new Handler<HttpServerRequest>() {
                @Override
                public void handle(HttpServerRequest event) {
                    String uri = event.path().toLowerCase();
                    Object res = "hello";
                    if (uri.startsWith(SHOW_INSTANCE_MONITOR_URL.toLowerCase())) {
                        instanceSnapshot = InstanceEntry.snapshot();
                        res = instanceSnapshot;
                    } else if (uri.startsWith(SHOW_DB_MONITOR_URL.toLowerCase())) {
                        databaseInstanceMapSnapshot = DatabaseInstanceEntry.snapshot();
                        res = databaseInstanceMapSnapshot;
                    } else if (uri.startsWith(SHOW_RW_MONITOR_URL.toLowerCase())) {
                        rwEntryMapSnapshot = RWEntry.snapshot();
                        res = rwEntryMapSnapshot;
                    } else if (uri.startsWith(QUERY_SQL_LOG.toLowerCase())) {
                        res = null;
                    }
                    event.response().end(Json.encode(res));
                }
            }).listen(port, ip);
        }

    }

    private void initBaseMonitor(Vertx vertx) {
        {
            TimerConfig instanceMonitorConfig = this.monitorConfig.getInstanceMonitor();
            TimeUnit timeUnit = TimeUnit.valueOf(instanceMonitorConfig.getTimeUnit());

            vertx.setTimer(timeUnit.toMillis(instanceMonitorConfig.getInitialDelay()), event -> vertx.setPeriodic(timeUnit.toMillis(instanceMonitorConfig.getPeriod()), event1 -> {
                instanceSnapshot = InstanceEntry.snapshot();
                InstanceEntry.reset();
            }));
        }


        {
            TimerConfig clusterConfig = this.monitorConfig.getClusterMonitor();
            TimeUnit timeUnit = TimeUnit.valueOf(clusterConfig.getTimeUnit());
            long readWriteRatioMillis = timeUnit.toMillis(clusterConfig.getPeriod());

            vertx.setTimer(timeUnit.toMillis(clusterConfig.getInitialDelay()), event -> vertx.setPeriodic(readWriteRatioMillis, event12 -> {
                rwEntryMapSnapshot = RWEntry.snapshot();
                RWEntry.reset();
            }));
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        {
            TimerConfig databaseInstanceMonitor = this.monitorConfig.getDatabaseInstanceMonitor();
            TimeUnit timeUnit = TimeUnit.valueOf(databaseInstanceMonitor.getTimeUnit());
            long databaseInstanceMonitorMillis = timeUnit.toMillis(databaseInstanceMonitor.getPeriod());
            vertx.setTimer(timeUnit.toMillis(databaseInstanceMonitor.getInitialDelay()), event -> vertx.setPeriodic(databaseInstanceMonitorMillis, event12 -> {
                databaseInstanceMapSnapshot = DatabaseInstanceEntry.snapshot();
                DatabaseInstanceEntry.reset();
            }));
        }
    }

    @Override
    protected void pushSqlLog(SqlEntry sqlEntry) {
        SqlLogConfig sqlLog = monitorConfig.getSqlLog();
        if (sqlLog != null && logConsumer != null) {
            if (sqlLog.getSqlTimeFilter() <= sqlEntry.getSqlTime()) {
                if (sqlLog.getSqlTypeFilter().contains(sqlEntry.getSqlType())) {
                    logConsumer.accept(sqlEntry);
                }
            }
        }
    }
}

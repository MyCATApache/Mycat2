package io.mycat.monitor;

import io.mycat.IOExecutor;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.MonitorConfig;
import io.mycat.config.SqlLogConfig;
import io.mycat.config.TimerConfig;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import lombok.SneakyThrows;

import java.util.Optional;
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
                    Object res = "mycat2 monitor";
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
                IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
                ioExecutor.executeBlocking((Handler<Promise<Void>>) promise -> {
                    try {
                        instanceSnapshot = InstanceEntry.snapshot();
                        InstanceEntry.reset();
                    } finally {
                        promise.tryComplete();
                    }
                });
            }));
        }


        {
            TimerConfig clusterConfig = this.monitorConfig.getClusterMonitor();
            TimeUnit timeUnit = TimeUnit.valueOf(clusterConfig.getTimeUnit());
            long readWriteRatioMillis = timeUnit.toMillis(clusterConfig.getPeriod());

            vertx.setTimer(timeUnit.toMillis(clusterConfig.getInitialDelay()), event -> vertx.setPeriodic(readWriteRatioMillis, event12 -> {
                IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
                ioExecutor.executeBlocking((Handler<Promise<Void>>) promise -> {
                    try {
                        rwEntryMapSnapshot = RWEntry.snapshot();
                        RWEntry.reset();
                    } finally {
                        promise.tryComplete();
                    }
                });
            }));
        }
        ////////////////////////////////////////////////////////////////////////////////////////////////////////////////
        {
            TimerConfig databaseInstanceMonitor = this.monitorConfig.getDatabaseInstanceMonitor();
            TimeUnit timeUnit = TimeUnit.valueOf(databaseInstanceMonitor.getTimeUnit());
            long databaseInstanceMonitorMillis = timeUnit.toMillis(databaseInstanceMonitor.getPeriod());
            vertx.setTimer(timeUnit.toMillis(databaseInstanceMonitor.getInitialDelay()), event -> vertx.setPeriodic(databaseInstanceMonitorMillis, event12 -> {
                IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
                ioExecutor.executeBlocking((Handler<Promise<Void>>) promise -> {
                    try {
                        databaseInstanceMapSnapshot = DatabaseInstanceEntry.snapshot();
                        DatabaseInstanceEntry.reset();
                    } finally {
                        promise.tryComplete();
                    }
                });
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

    @Override
    public void setSqlTimeFilter(long value) {
        Optional.ofNullable(this.monitorConfig).ifPresent(i -> i.getSqlLog().setSqlTimeFilter(value));
    }

    @Override
    public long getSqlTimeFilter() {
        return Optional.ofNullable(this.monitorConfig).map(i -> i.getSqlLog().getSqlTimeFilter()).orElse(Long.valueOf(-1));
    }
}

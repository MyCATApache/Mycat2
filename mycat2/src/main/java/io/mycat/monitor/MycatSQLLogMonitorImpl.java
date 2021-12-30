package io.mycat.monitor;

import io.mycat.IOExecutor;
import io.mycat.MetaClusterCurrent;
import io.mycat.config.MonitorConfig;
import io.mycat.config.SqlLogConfig;
import io.mycat.config.TimerConfig;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.json.Json;
import lombok.Data;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;

public class MycatSQLLogMonitorImpl extends MycatSQLLogMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(MycatSQLLogMonitorImpl.class);
    private final MonitorConfig monitorConfig;
    DatabaseInstanceEntry.DatabaseInstanceMap databaseInstanceMapSnapshot;
    InstanceEntry instanceSnapshot;
    RWEntry.RWEntryMap rwEntryMapSnapshot;
    Consumer<SqlEntry> logConsumer;

    public static final String SHOW_INSTANCE_MONITOR_URL = "/ShowInstanceMonitor";
    public static final String SHOW_DB_MONITOR_URL = "/ShowDbMonitor";
    public static final String SHOW_RW_MONITOR_URL = "/ShowRwMonitor";
    public static final String QUERY_SQL_LOG = "/QuerySqlLog";
    public static final String LOCKSERIVCE_URL = "/lockserivce";

    Map<String, LockContext> lockMap = new HashMap<>();

    @Data
    static class LockContext {
        String id;
        ReentrantLock lock;
    }

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
                public void handle(HttpServerRequest request) {
                    if(request.isExpectMultipart()){
                        request.setExpectMultipart(true);
                    }
                    request.endHandler(v -> {
                        vertx.executeBlocking(new Handler<Promise<Void>>() {
                            @Override
                            public void handle(Promise<Void> promise) {
                                try{
                                    MultiMap formAttributes = request.formAttributes();
                                    String uri = request.path().toLowerCase();
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
                                    } else if (uri.startsWith(LOCKSERIVCE_URL.toLowerCase())) {
                                        try {
                                            String method = Objects.toString(formAttributes.get("method")).toUpperCase();
                                            String name = formAttributes.get("name");
                                            String id = Objects.toString(formAttributes.get("id"));
                                            long timeout = Long.parseLong(Optional.ofNullable(formAttributes.get("timeout")).orElse("0"));
                                            synchronized (MycatSQLLogMonitorImpl.this) {
                                                switch (method) {
                                                    case "GET_LOCK": {
                                                        LockContext context = lockMap.computeIfAbsent(name, s -> {
                                                            LockContext lockContext = new LockContext();
                                                            lockContext.setId(id);
                                                            lockContext.setLock(new ReentrantLock());
                                                            return lockContext;
                                                        });
                                                        if (context.getId().equals(id)) {
                                                            res = 1;
                                                        } else {
                                                            boolean b = context.getLock().tryLock(timeout, TimeUnit.MILLISECONDS);
                                                           if (b){
                                                               context.setId(id);
                                                               res = 1;
                                                           }else {
                                                               res = 0;
                                                           }
                                                        }
                                                        break;
                                                    }
                                                    case "RELEASE_LOCK": {
                                                        if (!lockMap.containsKey(name)) {
                                                            res = null;
                                                        } else {
                                                            lockMap.remove(name);
                                                            res = 1;
                                                        }
                                                        break;
                                                    }
                                                    case "IS_FREE_LOCK": {
                                                        res = (!lockMap.containsKey(name)) ? 1 : 0;
                                                        break;
                                                    }
                                                    default:
                                                        res = ("Unexpected value: " + method);
                                                        break;
                                                }
                                            }
                                        } catch (Throwable throwable) {
                                            LOGGER.error("", throwable);
                                            res = throwable.getLocalizedMessage();
                                        }
                                    }
                                    request.response().end(Json.encode(res));
                                }catch (Throwable throwable){
                                    LOGGER.error("",throwable);
                                }finally {
                                    promise.tryComplete();
                                }
                            }
                        });

                    });
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

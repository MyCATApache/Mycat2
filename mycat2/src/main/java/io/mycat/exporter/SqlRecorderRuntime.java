package io.mycat.exporter;

import io.mycat.IOExecutor;
import io.mycat.MetaClusterCurrent;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.monitor.SqlEntry;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.TimeUnit;

public enum SqlRecorderRuntime implements SimpleAnalyzer {
    INSTANCE;
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlRecorderRuntime.class);
    public static long ONE_SECOND = TimeUnit.SECONDS.toMillis(1);
    private final ConcurrentLinkedDeque<SqlEntry> context = new ConcurrentLinkedDeque<>();


    SqlRecorderRuntime() {

    }

    @Override
    public List<SqlEntry> getRecords() {
        ArrayList<SqlEntry> sqlEntries = new ArrayList<>(context);
        Collections.sort(sqlEntries);
        return sqlEntries;
    }

    @Override
    public void addSqlRecord(SqlEntry record) {
        if (record != null) {
            boolean condition = record.getSqlTime() > (30 * ONE_SECOND);
            if (condition) {
                if (context.size() > 5000) {
                    IOExecutor ioExecutor = MetaClusterCurrent.wrapper(IOExecutor.class);
                    ioExecutor.executeBlocking((Handler<Promise<Void>>) promise -> {
                        synchronized (SqlRecorderRuntime.INSTANCE) {
                            if (context.size() > 5000) {
                                try {
                                    ArrayList<SqlEntry> sqlEntries = new ArrayList<>(context);
                                    Collections.sort(sqlEntries);
                                    context.clear();
                                    context.addAll(sqlEntries.subList(0, context.size() / 2));
                                } catch (Exception e) {
                                    LOGGER.warn("", e);
                                } finally {
                                    promise.tryComplete();
                                }
                            }
                        }
                    });
                }
            }
        }
    }
}
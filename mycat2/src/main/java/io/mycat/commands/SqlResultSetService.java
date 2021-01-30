package io.mycat.commands;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.spm.Plan;
import io.mycat.config.ServerConfig;
import io.mycat.config.SqlCacheConfig;
import io.mycat.config.ThreadPoolExecutorConfig;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.util.Dumper;
import io.mycat.util.TimeUnitUtil;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.linq4j.Enumerable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;

public class SqlResultSetService implements Closeable, Dumpable {
    final HashMap<String, SqlCacheTask> cacheConfigMap = new HashMap<>();
    final Cache<SQLSelectStatement, Object[]> cache = CacheBuilder.newBuilder().maximumSize(65535).build();
    final static Logger log = LoggerFactory.getLogger(SqlResultSetService.class);

    public synchronized void clear() {
        for (SqlCacheTask c : new ArrayList<>(cacheConfigMap.values())) {
            dropByName(c.getSqlCache().getName());
        }
        cache.invalidateAll();
        cache.cleanUp();
    }

    public synchronized void dropByName(String name) {
        if (name != null) {
            SqlCacheTask sqlCacheTask = cacheConfigMap.remove(name);
            if (sqlCacheTask != null) {
                if (!sqlCacheTask.scheduledFuture.isCancelled()) {
                    sqlCacheTask.scheduledFuture.cancel(false);
                }
                cache.invalidate(sqlCacheTask.getSqlSelectStatement());
            }
        }
    }

    public synchronized void addIfNotPresent(SqlCacheConfig sqlCache) {
        SqlCacheTask sqlCacheTask = cacheConfigMap.get(sqlCache.getName());
        if (sqlCacheTask != null) {
            if (sqlCacheTask.sqlCache.equals(sqlCache)) {
                return;
            }
            dropByName(sqlCache.getName());
        }
        TimeUnit timeUnit = TimeUnitUtil.valueOf(sqlCache.getTimeUnit());
        ScheduledExecutorService timer = ScheduleUtil.getTimer();
        String sql = sqlCache.getSql();
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);
        if (!(sqlStatement instanceof SQLSelectStatement)) {
            throw new MycatException("sql:{} not query statement", sql);
        }
        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) sqlStatement;
        ScheduledFuture<?> scheduledFuture = timer.scheduleAtFixedRate(() -> {
            try {
                MycatWorkerProcessor processor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
                NameableExecutor mycatWorker = processor.getMycatWorker();
                mycatWorker.execute(() -> {
                    try {
                        cache.invalidate(sqlSelectStatement);
                        loadResultSet(sqlSelectStatement);
                    } catch (Throwable throwable) {
                        log.error("", throwable);
                    }
                });
            } catch (Throwable throwable) {
                log.error("", throwable);
            }
        }, sqlCache.getInitialDelay(), sqlCache.getRefreshInterval(), timeUnit);
        cacheConfigMap.put(sqlCache.getName(),
                new SqlCacheTask(sqlSelectStatement, sqlCache, scheduledFuture)
        );
    }

    @Override
    public synchronized Dumper snapshot() {
        Dumper dumper = Dumper.create();
        cacheConfigMap.values().stream().map(i -> {
            String baseInfo = i.sqlCache.toString();
            boolean hasCache = null != cache.getIfPresent(i.getSqlSelectStatement());
            return baseInfo + " hasCache:" + hasCache;
        })
                .forEach(dumper::addText);
        return dumper;
    }

    @Getter
    static public class SqlCacheTask {
        final SqlCacheConfig sqlCache;
        final ScheduledFuture<?> scheduledFuture;
        final SQLSelectStatement sqlSelectStatement;

        public SqlCacheTask(SQLSelectStatement sqlSelectStatement,
                            SqlCacheConfig sqlCache,
                            ScheduledFuture<?> scheduledFuture) {
            this.sqlSelectStatement = sqlSelectStatement;
            this.sqlCache = sqlCache;
            this.scheduledFuture = scheduledFuture;
        }
    }

    public Optional<RowBaseIterator> get(SQLSelectStatement sqlSelectStatement) {
        if (cacheConfigMap.isEmpty()) {
            return Optional.empty();
        }
        ConcurrentMap<SQLSelectStatement, Object[]> map = cache.asMap();
        Object[] objects;
        if (!map.containsKey(sqlSelectStatement)) {
            return Optional.empty();
        }
        objects = cache.getIfPresent(sqlSelectStatement);
        if (objects == null) {
            objects = loadResultSet(sqlSelectStatement);
        }
        if (objects != null && objects.length == 2 && objects[0] != null && objects[1] != null) {

            List<Object[]> list = (List<Object[]>) objects[1];
            if (log.isDebugEnabled()) {
                log.debug("------------------------------------cache-----------------------------------");
                for (Object[] objects1 : list) {
                    log.debug(Arrays.toString(objects1));
                    int index = 0;
                    for (Object o : objects1) {
                        log.debug(index + "");
                        if (o == null) {
                            log.debug("null");
                        } else {
                            log.debug(o.getClass() + "");
                        }
                        index++;
                    }
                }
            }

            ResultSetBuilder.DefObjectRowIteratorImpl rowIterator =
                    new ResultSetBuilder.DefObjectRowIteratorImpl((MycatRowMetaData) objects[0], list.iterator());
            return Optional.of(rowIterator);
        } else {
            return Optional.empty();
        }
    }

    @SneakyThrows
    private Object[] loadResultSet( SQLSelectStatement sqlSelectStatement) {
        return cache.get(sqlSelectStatement, new Callable<Object[]>() {
            @Override
            public Object[] call() throws Exception {
                if (!MetaClusterCurrent.exist(DrdsRunner.class)) {
                    return new Object[2];
                }
                Object[] pair = new Object[2];
                MycatDataContext context = new MycatDataContextImpl();
                try {
                    DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
                    DrdsSql drdsSql = drdsRunner.preParse(sqlSelectStatement);
                    Plan plan = drdsRunner.getPlan(context, drdsSql);
                    ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
                    ThreadPoolExecutorConfig workerPool = serverConfig.getWorkerPool();
                    CompletableFuture<Enumerable<Object[]>> jdbcExecuter = DrdsRunner.getJdbcExecuter(plan, context, drdsSql.getParams());
                    Enumerable<Object[]> objects = jdbcExecuter.get(workerPool.getTaskTimeout(),TimeUnit.valueOf(workerPool.getTimeUnit()));
                    MycatRowMetaData metaData = new CalciteRowMetaData(plan.getPhysical().getRowType().getFieldList());
                    CopyMycatRowMetaData mycatRowMetaData = new CopyMycatRowMetaData(metaData);
                    int columnCount = metaData.getColumnCount();
                    ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
                    Iterator<Object[]> iterator = objects.iterator();
                    while (iterator.hasNext()) {
                        Object[] row = new Object[columnCount];
                        Object[] input = iterator.next();
                        for (int i = 0; i < columnCount; i++) {
                            row[i] = input[i];
                        }
                        builder.add(row);
                    }
                    ImmutableList<Object[]> objects1 = builder.build();
                    pair[0] = mycatRowMetaData;
                    pair[1] = objects1;
                    return (pair);
                } finally {
                    context.close();
                }
            }
        });
    }

    @Override
    public void close() {
        clear();
    }
}

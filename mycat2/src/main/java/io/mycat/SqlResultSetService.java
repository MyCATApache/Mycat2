package io.mycat;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.config.SqlCacheConfig;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt4.DefaultDatasourceFactory;
import io.mycat.hbt4.Executor;
import io.mycat.hbt4.ExecutorImplementorImpl;
import io.mycat.hbt4.MycatRel;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.hbt4.logical.rel.MycatInsertRel;
import io.mycat.hbt4.logical.rel.MycatUpdateRel;
import io.mycat.proxy.session.SimpleTransactionSessionRunner;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.sqlhandler.dml.DrdsRunners;
import io.mycat.util.Dumper;
import io.mycat.util.TimeUnitUtil;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
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
        if (objects != null &&objects.length==2&& objects[0] != null && objects[1] != null) {
            ResultSetBuilder.DefObjectRowIteratorImpl rowIterator =
                    new ResultSetBuilder.DefObjectRowIteratorImpl((MycatRowMetaData) objects[0], ((List<Object[]>) objects[1]).iterator());
            return Optional.of(rowIterator);
        } else {
            return Optional.empty();
        }
    }

    @SneakyThrows
    private Object[] loadResultSet(SQLSelectStatement sqlSelectStatement) {
        return cache.get(sqlSelectStatement, new Callable<Object[]>() {
            @Override
            public Object[] call() throws Exception {
                if (!MetaClusterCurrent.exist(DrdsRunner.class)){
                    return new Object[2];
                }
                Object[] pair = new Object[2];
                MycatDataContext context = new MycatDataContextImpl(new SimpleTransactionSessionRunner());
                try (DefaultDatasourceFactory defaultDatasourceFactory = new DefaultDatasourceFactory(context)) {
                    TempResultSetFactoryImpl tempResultSetFactory = new TempResultSetFactoryImpl();
                    ExecutorImplementorImpl executorImplementor = new ExecutorImplementorImpl(context, defaultDatasourceFactory, tempResultSetFactory) {
                        @Override
                        public void implementRoot(MycatRel rel, List<String> aliasList) {
                            if (rel instanceof MycatInsertRel) {
                                return;
                            }
                            if (rel instanceof MycatUpdateRel) {
                                return;
                            }
                            Executor executor = rel.implement(this);
                            RelDataType rowType = rel.getRowType();
                            EnumeratorRowIterator rowIterator = new EnumeratorRowIterator(new CalciteRowMetaData(rowType.getFieldList()),
                                    Linq4j.asEnumerable(() -> executor.outputObjectIterator()).enumerator(), () -> {
                            });
                            executor.open();
                            try {
                                MycatRowMetaData metaData = rowIterator.getMetaData();
                                CopyMycatRowMetaData mycatRowMetaData = new CopyMycatRowMetaData(metaData);
                                int columnCount = metaData.getColumnCount();
                                ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
                                while (rowIterator.next()) {
                                    Object[] row = new Object[columnCount];
                                    for (int i = 0; i < columnCount; i++) {
                                        row[i] = rowIterator.getObject(i + 1);
                                    }
                                    builder.add(row);
                                }
                                ImmutableList<Object[]> objects1 = builder.build();
                                pair[0] = mycatRowMetaData;
                                pair[1] = objects1;
                            } finally {
                                executor.close();
                            }
                        }
                    };
                    DrdsRunners.runOnDrds(context, sqlSelectStatement, executorImplementor);
                } finally {
                    context.close();
                }
                return (pair);
            }
        });
    }

    @Override
    public void close() {
        clear();
    }
}

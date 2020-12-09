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
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.*;

public class SqlResultSetService implements Closeable,Dumpable {
    final ConcurrentHashMap<String, SqlCacheTask> cacheConfigMap = new ConcurrentHashMap<>();
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
        if (sqlStatement instanceof SQLSelectStatement) {
            throw new MycatException("sql:{} not query statement", sql);
        }
        SQLSelectStatement sqlSelectStatement = (SQLSelectStatement) sqlStatement;
        ScheduledFuture<?> scheduledFuture = timer.scheduleAtFixedRate(() -> {
            try {
                MycatWorkerProcessor processor = MetaClusterCurrent.wrapper(MycatWorkerProcessor.class);
                NameableExecutor mycatWorker = processor.getMycatWorker();
                mycatWorker.execute(() -> {
                    cache.invalidate(sqlSelectStatement);
                    get(sqlSelectStatement);
                });
            } catch (Throwable throwable) {
                log.error("", throwable);
            }
        }, sqlCache.getRefreshInterval(), sqlCache.getInitialDelay(), timeUnit);
        cacheConfigMap.put(sqlCache.getName(),
                new SqlCacheTask(sqlSelectStatement, sqlCache, scheduledFuture)
        );
    }

    @Override
    public Dumper snapshot() {
        Dumper dumper = Dumper.create();
        cacheConfigMap.values().stream().map(i->i.sqlCache.toString())
                .forEach(c->dumper.addText(c));
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
        Object[] rowBaseIterator = cache.getIfPresent(sqlSelectStatement);
        if (rowBaseIterator != null) {
            try {
                Object[] objects = cache.get(sqlSelectStatement, new Callable<Object[]>() {
                    @Override
                    public Object[] call() throws Exception {
                        Object[] pair = new Object[2];
                        MycatDataContext context = new MycatDataContextImpl(new SimpleTransactionSessionRunner());
                        try (DefaultDatasourceFactory defaultDatasourceFactory = new DefaultDatasourceFactory(context)) {
                            TempResultSetFactoryImpl tempResultSetFactory = new TempResultSetFactoryImpl();
                            ExecutorImplementorImpl executorImplementor = new ExecutorImplementorImpl(defaultDatasourceFactory, tempResultSetFactory) {
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
                                    MycatRowMetaData metaData = rowIterator.getMetaData();
                                    CopyMycatRowMetaData mycatRowMetaData = new CopyMycatRowMetaData(metaData);
                                    int columnCount = metaData.getColumnCount();
                                    ImmutableList.Builder<Object[]> builder = ImmutableList.builder();
                                    while (rowIterator.next()) {
                                        Object[] row = new Object[columnCount];
                                        for (int i = 0; i < columnCount; i++) {
                                            row[i] = rowIterator.getObject(i + 1);
                                            builder.add(row);
                                        }
                                    }
                                    ImmutableList<Object[]> objects = builder.build();
                                    pair[0] = mycatRowMetaData;
                                    pair[1] = objects;
                                }
                            };
                            DrdsRunners.runOnDrds(context, sqlSelectStatement, executorImplementor);
                        } finally {
                            context.close();
                        }
                        return pair;
                    }
                });
                ResultSetBuilder.DefObjectRowIteratorImpl rowIterator =
                        new ResultSetBuilder.DefObjectRowIteratorImpl((MycatRowMetaData) objects[0], ((List<Object[]>) objects[1]).iterator());
                return Optional.ofNullable(rowIterator);
            } catch (ExecutionException e) {
                log.error("can not get cache sql:{}", sqlSelectStatement, e);
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void close() {
        clear();
    }
}

package io.mycat.commands;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import io.mycat.*;
import io.mycat.api.collector.MysqlPayloadObject;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.JdbcConnectionUsage;
import io.mycat.calcite.ProxyConnectionUsage;
import io.mycat.calcite.plan.ObservablePlanImplementorImpl;
import io.mycat.calcite.spm.Plan;
import io.mycat.config.ServerConfig;
import io.mycat.config.SqlCacheConfig;
import io.mycat.config.ThreadPoolExecutorConfig;
import io.mycat.runtime.MycatDataContextImpl;
import io.mycat.runtime.ProxyTransactionSession;
import io.mycat.util.Dumper;
import io.mycat.util.TimeUnitUtil;
import io.reactivex.rxjava3.core.Observable;
import lombok.Getter;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.*;
import java.util.concurrent.*;

public class  SqlResultSetService implements Closeable, Dumpable {
    final HashMap<String, SqlCacheTask> cacheConfigMap = new HashMap<>();
    final Cache<SQLSelectStatement, Optional<Observable<MysqlPayloadObject>>> cache = CacheBuilder.newBuilder().maximumSize(65535).build();
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

    public Optional<Observable<MysqlPayloadObject>> get(SQLSelectStatement sqlSelectStatement) {
        if (cacheConfigMap.isEmpty()) {
            return Optional.empty();
        }
        ConcurrentMap<SQLSelectStatement, Optional<Observable<MysqlPayloadObject>>> map = cache.asMap();
        Optional<Observable<MysqlPayloadObject>> optionalObservable;
        if (!map.containsKey(sqlSelectStatement)) {
            return Optional.empty();
        }
        optionalObservable = cache.getIfPresent(sqlSelectStatement);
        if (optionalObservable == null) {
         return loadResultSet(sqlSelectStatement);
        } else {
            return Optional.empty();
        }
    }

    @SneakyThrows
    private Optional<Observable<MysqlPayloadObject>> loadResultSet(SQLSelectStatement sqlSelectStatement) {
        return cache.get(sqlSelectStatement, () -> {
            if (!MetaClusterCurrent.exist(DrdsRunner.class)) {
                return Optional.empty();
            }
            MycatDataContext context = new MycatDataContextImpl();
            try {
                DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
                DrdsSql drdsSql = drdsRunner.preParse(sqlSelectStatement);
                Plan plan = drdsRunner.getPlan(context, drdsSql);
                CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
                ProxyTransactionSession transactionSession = (ProxyTransactionSession) context.getTransactionSession();
                ProxyConnectionUsage connectionUsage = JdbcConnectionUsage
                        .computeProxyTargetConnection(context, drdsSql.getParams(), codeExecuterContext);
                        connectionUsage.collect((ProxyTransactionSession)context.getTransactionSession(), drdsSql.getParams());
                ObservablePlanImplementorImpl planImplementor = new ObservablePlanImplementorImpl(
                        transactionSession,
                        context, drdsSql.getParams(), null);
                Observable<MysqlPayloadObject> observable = planImplementor.getMysqlPayloadObjectObservable(context, drdsSql.getParams(), plan);
                observable= observable.cache();
                return Optional.ofNullable(observable);
            } finally {
                context.close();
            }
        });
    }

    @Override
    public void close() {
        clear();
    }
}

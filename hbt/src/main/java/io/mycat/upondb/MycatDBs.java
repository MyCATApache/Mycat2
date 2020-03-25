package io.mycat.upondb;

import io.mycat.Identical;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.metadata.MetadataManager;
import io.mycat.util.SQLContext;
import io.mycat.util.SQLContextImpl;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

public class MycatDBs {


    public static MycatDBClientMediator createClient(MycatDataContext dataContext) {
        return new MycatDBClientMediator() {
            final HashMap<String, Object> cache = new HashMap<>();

            @Override
            public MycatDBSharedServer getUponDBSharedServer() {
                return new MycatDBSharedServerImpl();
            }

            @Override
            public MycatDBClientBasedConfig config() {
                MycatDBClientBasedConfig mycatDBClientBasedConfig = new MycatDBClientBasedConfig(MetadataManager.INSTANCE.getLogicTableMap(), null);
                return mycatDBClientBasedConfig ;
            }

            @Override
            public Map<String, Object> variables() {
                return Collections.emptyMap();
            }

            @Override
            public <T> T getCache(Identical key, String targetName, String sql, List<Object> params) {
                return (T) cache.get(targetName + sql);
            }

            @Override
            public <T> T getCacheCountDownByIdentity(Identical key, String targetName, String sql, List<Object> params) {
                return null;
            }

            @Override
            public void cache(Identical key, String targetName, String sql, List<Object> params, Supplier<Object> o) {
                Object o1 = cache.computeIfAbsent(targetName + sql, s -> o.get());
            }

            @Override
            public <T> T removeCache(Identical key, String targetName, String sql, List<Object> params) {
                return (T) cache.remove(targetName + sql);
            }

            @Override
            public RowBaseIterator prepareQuery(String targetName, String sql, List<Object> params) {
                throw new UnsupportedOperationException();
            }

            @Override
            public UpdateRowIteratorResponse prepareUpdate(String targetName, String sql, List<Object> params) {
                throw new UnsupportedOperationException();
            }

            @Override
            public UpdateRowIteratorResponse update(String targetName, String sql) {
                return dataContext.update(targetName, sql);
            }

            @Override
            public RowBaseIterator query(String targetName, String sql) {
                return dataContext.query(targetName, sql);
            }

            @Override
            public RowBaseIterator queryDefaultTarget(String sql) {
                return dataContext.queryDefaultTarget(sql);
            }

            @Override
            public UpdateRowIteratorResponse updateDefaultTarget(String sql) {
                return null;
            }

            @Override
            public UpdateRowIteratorResponse update(String targetName, List<String> sqls) {
                long updateCount = 0;
                long lastInsertId = 0;
                for (String sql : sqls) {
                    UpdateRowIteratorResponse mycatUpdateResponse = update(targetName, sql);
                    updateCount += mycatUpdateResponse.getUpdateCount();
                    lastInsertId = Math.max(mycatUpdateResponse.getLastInsertId(), lastInsertId);
                }
                return new UpdateRowIteratorResponse(updateCount, lastInsertId, dataContext.getTransactionSession().getServerStatus());
            }

            @Override
            public void begin() {
                dataContext.getTransactionSession().begin();
            }

            @Override
            public void rollback() {
                dataContext.getTransactionSession().rollback();
            }

            @Override
            public void commit() {
                dataContext.getTransactionSession().commit();
            }

            @Override
            public void setTransactionIsolation(int value) {
                dataContext.getTransactionSession().setTransactionIsolation(value);
            }

            @Override
            public int getTransactionIsolation() {
                return dataContext.getTransactionSession().getTransactionIsolation();
            }

            @Override
            public boolean isAutocommit() {
                return dataContext.isAutocommit();
            }

            @Override
            public void setAutocommit(boolean autocommit) {
                dataContext.setAutoCommit(autocommit);
            }

            @Override
            public boolean isAutoCommit() {
                return dataContext.getTransactionSession().isAutocommit();
            }

            @Override
            public boolean isInTransaction() {
                return dataContext.isInTransaction();
            }

            @Override
            public void setAutoCommit(boolean autocommit) {
                dataContext.setAutoCommit(autocommit);
            }

            @Override
            public void close() {

            }

            @Override
            public void recycleResource() {
                cache.clear();
            }


            @Override
            public int getServerStatus() {
                return dataContext.getTransactionSession().getServerStatus();
            }

            @Override
            public SQLContext sqlContext() {
                return new SQLContextImpl(this);
            }

            @Override
            public AtomicBoolean cancelFlag() {
                return dataContext.getCancelFlag();
            }

            @Override
            public String resolveFinalTargetName(String targetName) {
                return dataContext.resolveDatasourceTargetName(targetName);
            }
        };
    }
}
package io.mycat.upondb;

import io.mycat.Identical;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIterator;
import io.mycat.beans.resultset.MycatUpdateResponse;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.TransactionSessionUtil;
import io.mycat.metadata.LogicTable;
import io.mycat.metadata.MetadataManager;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class MycatDBs {


    public static MycatDBClientMediator createClient() {
        return new MycatDBClientMediator() {
            final IdentityHashMap<String, Object> cache = new IdentityHashMap<>();
            final AtomicBoolean cancelFlag = new AtomicBoolean(false);

            @Override
            public MycatDBSharedServer getUponDBSharedServer() {
                return new MycatDBSharedServerImpl();
            }

            @Override
            public Map<String, Map<String, LogicTable>> config() {
                return (Map) MetadataManager.INSTANCE.getLogicTableMap();
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
            public void cache(Identical key, String targetName, String sql, List<Object> params,Object o) {
                 cache.put(targetName+sql,o);
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
            public UpdateRowIterator prepareUpdate(String targetName, String sql, List<Object> params) {
                throw new UnsupportedOperationException();
            }

            @Override
            public UpdateRowIterator update(String targetName, String sql) {
                MycatUpdateResponse mycatUpdateResponse = TransactionSessionUtil.executeUpdateByReplicaName(targetName, sql, true, null);
                return new UpdateRowIterator(mycatUpdateResponse.getUpdateCount(), mycatUpdateResponse.getLastInsertId());
            }

            @Override
            public RowBaseIterator query(String targetName, String sql) {
                DefaultConnection connectionByReplicaName = TransactionSessionUtil.getConnectionByReplicaName(targetName, false, null);
                return connectionByReplicaName.executeQuery(sql);
            }

            @Override
            public UpdateRowIterator update(String targetName, List<String> sqls) {
                DefaultConnection connectionByReplicaName = TransactionSessionUtil.getConnectionByReplicaName(targetName, true, null);
                long updateCount = 0;
                long lastInsertId = 0;
                for (String sql : sqls) {
                    MycatUpdateResponse mycatUpdateResponse = connectionByReplicaName.executeUpdate(sql, true);
                    updateCount += mycatUpdateResponse.getUpdateCount();
                    lastInsertId = Math.max(mycatUpdateResponse.getLastInsertId(), lastInsertId);
                }
                return new UpdateRowIterator(updateCount, lastInsertId);
            }

            @Override
            public void begin() {
                TransactionSessionUtil.begin();
            }

            @Override
            public void rollback() {
                TransactionSessionUtil.rollback();
            }

            @Override
            public void commit() {
                TransactionSessionUtil.commit();
            }

            @Override
            public void setTransactionIsolation(int value) {
                TransactionSessionUtil.setIsolation(value);
            }

            @Override
            public int getTransactionIsolation() {
                return TransactionSessionUtil.getTransactionIsolation();
            }

            @Override
            public boolean isAutocommit() {
                return TransactionSessionUtil.isAutocommit();
            }

            @Override
            public void setAutocommit(boolean autocommit) {
                TransactionSessionUtil.setAutocommit(autocommit);
            }

            @Override
            public void close() {
                TransactionSessionUtil.reset();
            }

            @Override
            public void recycleResource() {
                cache.clear();
            }


            @Override
            public int getServerStatus() {
                return TransactionSessionUtil.currentTransactionSession().getServerStatus();
            }

            @Override
            public AtomicBoolean cancleFlag() {
                return cancelFlag;
            }
        };
    }
}
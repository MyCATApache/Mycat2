package io.mycat.mock;

import io.mycat.Identical;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.JdbcRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.upondb.MycatDBClientBasedConfig;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBSharedServer;
import io.mycat.util.SQLContext;
import lombok.AllArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

@AllArgsConstructor
public class MockDBClientMediator extends MycatDBClientMediator {
    final static Logger log = LoggerFactory.getLogger(MockDBClientMediator.class);
    final MockDBSharedServer sharedServer = new MockDBSharedServer();
    final MycatDBClientBasedConfig clientBasedConfig;
    @Override
    public MycatDBSharedServer getUponDBSharedServer() {
        return sharedServer;
    }

    @Override
    public MycatDBClientBasedConfig config() {
        return clientBasedConfig;
    }

    @Override
    public Map<String, Object> variables() {
        return null;
    }

    @Override
    public <T> T getCache(Identical key, String targetName, String sql, List<Object> params) {
        return null;
    }

    @Override
    public <T> T getCacheCountDownByIdentity(Identical key, String targetName, String sql, List<Object> params) {
        return null;
    }

    @Override
    public void cache(Identical key, String targetName, String sql, List<Object> params, Supplier<Object> o) {

    }

    @Override
    public <T> T removeCache(Identical key, String targetName, String sql, List<Object> params) {
        return null;
    }

    @Override
    public RowBaseIterator prepareQuery(String targetName, String sql, List<Object> params) {
        return null;
    }

    @Override
    public UpdateRowIteratorResponse prepareUpdate(String targetName, String sql, List<Object> params) {
        return null;
    }

    @Override
    public UpdateRowIteratorResponse update(String targetName, String sql) {
        return null;
    }

    @Override
    public RowBaseIterator query(MycatRowMetaData mycatRowMetaData, String targetName, String sql) {
        return null;
    }

    @Override
    public RowBaseIterator query(String targetName, String sql) {
        return null;
    }

    @Override
    public MycatRowMetaData queryMetaData(String targetName, String sql) {
        String datasourceName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(targetName, true, null);
        JdbcDataSource jdbcDataSource = JdbcRuntime.INSTANCE.getConnectionManager().getDatasourceInfo().get(datasourceName);
        try(Connection connection1 = jdbcDataSource.getDataSource().getConnection()){
            try(Statement statement = connection1.createStatement()){
                statement.setMaxRows(0);
                try(ResultSet resultSet = statement.executeQuery(sql)){
                    return new JdbcRowMetaData(resultSet.getMetaData());
                }
            }
        } catch (SQLException e) {
            log.warn("{}",e);
        }
        return null;
    }

    @Override
    public RowBaseIterator queryDefaultTarget(String sql) {
        return null;
    }

    @Override
    public UpdateRowIteratorResponse updateDefaultTarget(String sql) {
        return null;
    }

    @Override
    public UpdateRowIteratorResponse update(String targetName, List<String> sqls) {
        return null;
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public void begin() {

    }

    @Override
    public void rollback() {

    }

    @Override
    public void useSchema(String normalize) {

    }

    @Override
    public void commit() {

    }

    @Override
    public void setTransactionIsolation(int value) {

    }

    @Override
    public int getTransactionIsolation() {
        return 0;
    }

    @Override
    public boolean isAutocommit() {
        return false;
    }

    @Override
    public void setAutocommit(boolean autocommit) {

    }

    @Override
    public boolean isAutoCommit() {
        return false;
    }

    @Override
    public boolean isInTransaction() {
        return false;
    }

    @Override
    public long getMaxRow() {
        return 0;
    }

    @Override
    public void setMaxRow(long value) {

    }

    @Override
    public void setAutoCommit(boolean autocommit) {

    }

    @Override
    public void close() {

    }

    @Override
    public AtomicBoolean cancelFlag() {
        return null;
    }

    @Override
    public String resolveFinalTargetName(String targetName) {
        return targetName;
    }

    @Override
    public void recycleResource() {

    }

    @Override
    public int getServerStatus() {
        return 0;
    }

    @Override
    public SQLContext sqlContext() {
        return null;
    }

    @Override
    public long lastInsertId() {
        return 0;
    }

    @Override
    public void setVariable(String target, Object value) {

    }

    @Override
    public Object getVariable(String target) {
        return null;
    }
}
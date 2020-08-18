package io.mycat.hbt4;

import com.alibaba.fastsql.util.JdbcUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.*;
import io.mycat.api.collector.ComposeFutureRowBaseIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.JdbcRowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.resultset.MyCatResultSetEnumerator;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.datasource.jdbc.datasource.JdbcDataSource;
import io.mycat.mpp.Row;
import lombok.SneakyThrows;
import org.apache.calcite.sql.util.SqlString;
import org.checkerframework.checker.signature.qual.SourceNameForNonArrayNonInner;

import java.sql.PreparedStatement;
import java.sql.*;
import java.util.*;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class DefaultDatasourceFactory implements DatasourceFactory {
    final MycatDataContext context;
    final List<String> targets = new ArrayList<>();
    private Map<String, Deque<MycatConnection>> connectionMap;
    private LinkedList<MycatConnection> autoCloseables = new LinkedList<>();
    public DefaultDatasourceFactory(MycatDataContext context) {
        this.context = context;
    }

    @Override
    public void close() throws Exception {
        for (MycatConnection autoCloseable : autoCloseables) {
            if(!autoCloseable.isClosed()){
                autoCloseable.close();
            }
        }
    }

    @Override
    public void open() {
        this.connectionMap = context.getTransactionSession().getConnection(targets);
    }

    @Override
    public void createTableIfNotExisted(String targetName, String createTableSql) {

    }


    @Override
    public Map<String, Connection> getConnections(List<String> targets) {
        HashMap<String,Connection> connectionHashMap = new HashMap<>();
        Map<String, Deque<MycatConnection>> connection = context.getTransactionSession().getConnection(targets);
        for (Map.Entry<String, Deque<MycatConnection>> stringDequeEntry : connection.entrySet()) {
            connectionHashMap.put(stringDequeEntry.getKey(),stringDequeEntry.getValue().getFirst().unwrap(Connection.class));
        }

        return connectionHashMap;
    }

    @Override
    public void regist(ImmutableList<String> asList) {
        targets.addAll(asList);
    }

    @Override
    public Connection getConnection(String key) {
        Deque<MycatConnection> mycatConnections = connectionMap.get(key);
        MycatConnection pop = mycatConnections.pop();
        autoCloseables.add(pop);
        return pop.unwrap(Connection.class);
    }

    /**
     * @todo check dead lock
     * @param targets
     * @return
     */
    @Override
    @SneakyThrows
    public List<Connection> getTmpConnections(List<String> targets) {
        List<Connection> res = new ArrayList<>();
        JdbcConnectionManager connectionManager = JdbcRuntime.INSTANCE.getConnectionManager();
        Map<String, JdbcDataSource> datasourceInfo = connectionManager.getDatasourceInfo();
        synchronized (JdbcRuntime.INSTANCE) {
            for (String jdbcDataSource : targets) {
                JdbcDataSource dataSource = datasourceInfo.get(jdbcDataSource);
                res.add(  dataSource.getDataSource().getConnection());
            }
            return res;
        }
    }

    @Override
    @SneakyThrows
    public void recycleTmpConnections(List<Connection> connections) {
        for (Connection connection : connections) {
            JdbcUtils.close(connection);
        }

    }
}
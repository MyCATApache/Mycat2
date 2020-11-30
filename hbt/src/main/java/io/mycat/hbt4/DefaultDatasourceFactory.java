package io.mycat.hbt4;

import com.google.common.collect.ImmutableList;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatConnection;
import io.mycat.MycatDataContext;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import lombok.SneakyThrows;

import java.util.*;

public class DefaultDatasourceFactory implements DataSourceFactory {
    final MycatDataContext context;
    final List<String> targets = new ArrayList<>();
    private Map<String, Deque<MycatConnection>> connectionMap;

    public DefaultDatasourceFactory(MycatDataContext context) {
        this.context = context;
    }

    @Override
    public void close() throws Exception {
        context.getTransactionSession().clearJdbcConnection();
    }

    @Override
    public void open() {
        this.connectionMap = context.getTransactionSession().getConnection(targets);
    }


    @Override
    public Map<String, MycatConnection> getConnections(List<String> targets) {
        HashMap<String,MycatConnection> connectionHashMap = new HashMap<>();
        Map<String, Deque<MycatConnection>> connection = context.getTransactionSession().getConnection(targets);
        for (Map.Entry<String, Deque<MycatConnection>> stringDequeEntry : connection.entrySet()) {
            connectionHashMap.put(stringDequeEntry.getKey(),stringDequeEntry.getValue().getFirst());
        }

        return connectionHashMap;
    }

    @Override
    public void registered(ImmutableList<String> asList) {
        targets.addAll(asList);
    }

    @Override
    public MycatConnection getConnection(String key) {
        Deque<MycatConnection> mycatConnections = connectionMap.get(key);
        MycatConnection pop = mycatConnections.pop();
        return pop;
    }

    /**
     * @todo check dead lock
     * @param targets
     * @return
     */
    @Override
    @SneakyThrows
    public List<MycatConnection> getTmpConnections(List<String> targets) {
        List<MycatConnection> res = new ArrayList<>();
        JdbcConnectionManager connectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        synchronized (connectionManager) {
            for (String jdbcDataSource : targets) {
                res.add(connectionManager.getConnection(jdbcDataSource));
            }
            return res;
        }
    }

    @Override
    @SneakyThrows
    public void recycleTmpConnections(List<MycatConnection> connections) {
        for (MycatConnection connection : connections) {
            connection.close();
        }

    }
}
package io.mycat.hbt4;

import com.google.common.collect.ImmutableList;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatConnection;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import lombok.SneakyThrows;

import java.util.*;

public class DefaultDatasourceFactory implements DataSourceFactory {
    final MycatDataContext context;
    final List<String> targets = new ArrayList<>();
    private Map<String, Deque<MycatConnection>> connectionMap = new HashMap<>();
    private ArrayList<MycatConnection> needClosed = new ArrayList<>();

    public DefaultDatasourceFactory(MycatDataContext context) {
        this.context = context;
    }

    @Override
    public void close() throws Exception {
//       .clearJdbcConnection(); clear in  SQLExecuterWriter
//        for (MycatConnection mycatConnection : needClosed) {
//            mycatConnection.close();
//        }
        TransactionSession transactionSession = context.getTransactionSession();
        for (MycatConnection mycatConnection : needClosed) {
            transactionSession.addCloseResource(mycatConnection);
        }
    }

    @Override
    public void open() {
        if (connectionMap.isEmpty()){
            TransactionSession transactionSession = context.getTransactionSession();
            JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
            List<String> all = new ArrayList<>(targets);
            HashSet<String> keys = new HashSet<>(targets);
            synchronized (jdbcConnectionManager) {
                for (String key : keys) {
                    Deque<MycatConnection> mycatConnections = connectionMap.computeIfAbsent(key, (k) -> new LinkedList<>());
                    mycatConnections.add(transactionSession.getConnection(key));
                    all.remove(key);
                }
                for (String key : all) {
                    DefaultConnection connection = jdbcConnectionManager.getConnection(key);
                    Deque<MycatConnection> mycatConnections = connectionMap.get(key);
                    mycatConnections.add(connection);
                    needClosed.add(connection);
                }
            }
        }

    }


    @Override
    public Map<String, MycatConnection> getConnections(List<String> targets) {
        HashMap<String, MycatConnection> connectionHashMap = new HashMap<>();
        for (Map.Entry<String, Deque<MycatConnection>> stringDequeEntry : connectionMap.entrySet()) {
            if (stringDequeEntry.getValue().size() == 1) {
                connectionHashMap.put(stringDequeEntry.getKey(), stringDequeEntry.getValue().getFirst());
            } else {
                throw new IllegalArgumentException();
            }
        }
        return connectionHashMap;
    }

    @Override
    public void registered(List<String> asList) {
        targets.addAll(asList);
    }

    @Override
    public MycatConnection getConnection(String key) {
        Deque<MycatConnection> mycatConnections = connectionMap.get(key);
        MycatConnection pop = mycatConnections.pop();
        return pop;
    }

    /**
     * @param targets
     * @return
     * @todo check dead lock
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
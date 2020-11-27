package io.mycat.datasource.jdbc.transactionsession;

import com.google.common.collect.ImmutableMap;
import io.mycat.*;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.replica.DataSourceNearnessImpl;
import io.mycat.util.Dumper;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class TransactionSessionTemplate implements TransactionSession {
    protected final Map<String, DefaultConnection> updateConnectionMap = new ConcurrentHashMap<>();
    protected final DataSourceNearness dataSourceNearness = new DataSourceNearnessImpl(this);
    final MycatDataContext dataContext;
    protected final ConcurrentLinkedQueue<AutoCloseable> closeResourceQueue = new ConcurrentLinkedQueue<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConnectionManager.class);

    public TransactionSessionTemplate(MycatDataContext dataContext) {
        this.dataContext = dataContext;
    }

    public boolean isInTransaction() {
        return dataContext.isInTransaction();
    }

    public void setAutocommit(boolean autocommit) {
        dataContext.setAutoCommit(autocommit);
    }

    public boolean isAutocommit() {
        return dataContext.isAutocommit();
    }

    public void begin() {
        if (!isInTransaction() && !updateConnectionMap.isEmpty()) {
            throw new IllegalArgumentException("存在连接泄漏");
        }
        if (!isInTransaction()) {
            callBackBegin();
        }
        dataContext.setInTransaction(true);
    }

    public void commit() {
        if (isInTransaction() && !updateConnectionMap.isEmpty()) {//真正开启事务才提交
            callBackCommit();
        }
        setInTranscation(false);
        updateConnectionMap.forEach((key, value) -> value.close());
        updateConnectionMap.clear();
    }

    public void rollback() {
        if (isInTransaction() && !updateConnectionMap.isEmpty()) {
            callBackRollback();
        }
        setInTranscation(false);
        updateConnectionMap.forEach((key, value) -> value.close());
        updateConnectionMap.clear();
    }

    /**
     * 模拟autocommit = 0 时候自动开启事务
     */
    public void doAction() {
        if (!isAutocommit()) {
            begin();
        }
    }

    abstract protected void callBackBegin();

    abstract protected void callBackCommit();

    abstract protected void callBackRollback();


    public int getServerStatus() {
        return dataContext.serverStatus();
    }


    public boolean isReadOnly() {
        return dataContext.isReadOnly();
    }


    public void setReadOnly(boolean readOnly) {
        this.updateConnectionMap.forEach((key, value) -> value.setReadyOnly(readOnly));
    }


    private void setInTranscation(boolean inTranscation) {
        this.dataContext.setInTransaction(inTranscation);
    }

    public synchronized void close() {
        clearJdbcConnection();
        for (Map.Entry<String, DefaultConnection> stringDefaultConnectionEntry : updateConnectionMap.entrySet()) {
            DefaultConnection value = stringDefaultConnectionEntry.getValue();
            if (value != null) {
                value.close();
            }
        }
        updateConnectionMap.clear();
        dataSourceNearness.clear();
    }

    @Override
    public String resolveFinalTargetName(String targetName) {
        return dataSourceNearness.getDataSourceByTargetName(targetName);
    }

    public int getTransactionIsolation() {
        return dataContext.getIsolation().getJdbcValue();
    }

    @Override
    @SneakyThrows
    public void clearJdbcConnection() {
        if (!isInTransaction()) {
            Set<Map.Entry<String, DefaultConnection>> entries = updateConnectionMap.entrySet();
            for (Map.Entry<String, DefaultConnection> entry : entries) {
                DefaultConnection value = entry.getValue();
                if (value != null) {
                    value.close();
                }
            }
            updateConnectionMap.clear();
            dataSourceNearness.clear();
        }
        Iterator<AutoCloseable> iterator = closeResourceQueue.iterator();
        while (iterator.hasNext()) {
            iterator.next().close();
            iterator.remove();
        }
    }

    public void setTransactionIsolation(int transactionIsolation) {
        this.dataContext.setIsolation(MySQLIsolation.parseJdbcValue(transactionIsolation));
        this.updateConnectionMap.forEach((key, value) -> value.setTransactionIsolation(transactionIsolation));
    }

    public void reset() {
        for (Map.Entry<String, DefaultConnection> stringDefaultConnectionEntry : updateConnectionMap.entrySet()) {
            DefaultConnection value = stringDefaultConnectionEntry.getValue();
            if (value != null) {
                value.close();
            }
        }
        this.updateConnectionMap.clear();
        this.dataSourceNearness.clear();
    }

    @Override
    public void addCloseResource(AutoCloseable closeable) {
        closeResourceQueue.add(closeable);
    }

    @Override
    public Dumper snapshot() {
        return Dumper.create()
                .addText("jdbcCon", String.join(",", this.updateConnectionMap.keySet()))
                .addText("closeQueueSize", String.valueOf(closeResourceQueue.size()));
    }

    public Map<String, Deque<MycatConnection>> getConnection(List<String> targetNames) {
        return callBackConnections(targetNames, false, Connection.TRANSACTION_REPEATABLE_READ, false);
    }

    protected Map<String, Deque<MycatConnection>> callBackConnections(List<String> jdbcDataSources,
                                                                      boolean autocommit,
                                                                      int transactionIsolation,
                                                                      boolean readOnly) {
        if (jdbcDataSources.size() == 1) {
            String jdbcDataSource = jdbcDataSources.get(0);
            MycatConnection defaultConnection = updateConnectionMap.compute(jdbcDataSource,
                    (dataSource, absractConnection) -> {
                        if (absractConnection != null && !absractConnection.isClosed()) {
                            return absractConnection;
                        } else {
                            return getConnection(jdbcDataSource, autocommit, transactionIsolation, readOnly);
                        }
                    });
            LinkedList<MycatConnection> linkedList = new LinkedList<>();
            linkedList.add(defaultConnection);
            return ImmutableMap.of(jdbcDataSource, linkedList);
        }
        Map<String, Deque<MycatConnection>> res = new HashMap<>();
        List<String> needAdd = new ArrayList<>();
        for (String key : jdbcDataSources) {
            Deque<MycatConnection> mycatConnections = res.computeIfAbsent(key, s -> new LinkedList<>());
            if (mycatConnections.isEmpty()) {
                MycatConnection connection = updateConnectionMap.get(key);
                if (connection != null) {
                    mycatConnections.add(connection);
                } else {
                    needAdd.add(key);
                }
            } else {
                needAdd.add(key);
            }
        }
        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        synchronized (jdbcConnectionManager) {
            for (String jdbcDataSource : needAdd) {
                Deque<MycatConnection> mycatConnections = res.computeIfAbsent(jdbcDataSource, s -> new LinkedList<>());
                DefaultConnection connection = getConnection(jdbcDataSource, autocommit, transactionIsolation, readOnly);
//                addCloseResource(connection);
                mycatConnections.add(connection);
            }
            return res;
        }
    }

    abstract public DefaultConnection getConnection(String name, Boolean autocommit, int transactionIsolation, boolean readOnly);
}
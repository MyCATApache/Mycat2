package io.mycat.datasource.jdbc.transactionSession;

import io.mycat.DataSourceNearness;
import io.mycat.MycatConnection;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.replica.DataSourceNearnessImpl;
import io.mycat.util.Dumper;
import lombok.SneakyThrows;

import java.sql.Connection;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

public abstract class TransactionSessionTemplate implements TransactionSession {
    protected final Map<String, DefaultConnection> updateConnectionMap = new ConcurrentHashMap<>();
    protected final DataSourceNearness dataSourceNearness = new DataSourceNearnessImpl(this);
    final MycatDataContext dataContext;
    protected final ConcurrentLinkedQueue<AutoCloseable> closeResourceQueue = new ConcurrentLinkedQueue<>();

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

    @Override
    public MycatConnection getConnection(
            String targetName) {
        doAction();
        String dataSourceByTargetName = Objects.requireNonNull(dataSourceNearness.getDataSourceByTargetName(targetName));
        return callBackConnection(dataSourceByTargetName, isAutocommit(), getTransactionIsolation(), isReadOnly());
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

    abstract protected MycatConnection callBackConnection(String jdbcDataSource, boolean autocommit, int transactionIsolation, boolean readOnly);

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
        check();
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
    public void check() {
        if (!isInTransaction()) {
            Set<Map.Entry<String, DefaultConnection>> entries = updateConnectionMap.entrySet();
            for (Map.Entry<String, DefaultConnection> entry : entries) {
                Connection rawConnection = entry.getValue().getRawConnection();
                if (!rawConnection.getAutoCommit()) {
                    rawConnection.rollback();
                }
                rawConnection.close();
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
}
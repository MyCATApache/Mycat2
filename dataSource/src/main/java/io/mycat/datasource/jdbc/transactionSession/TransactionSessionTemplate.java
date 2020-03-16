package io.mycat.datasource.jdbc.transactionSession;

import io.mycat.beans.mysql.MySQLServerStatusFlags;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

public abstract class TransactionSessionTemplate {
    protected final Map<String, DefaultConnection> updateConnectionMap = new HashMap<>();
    protected volatile boolean autocommit = true;
    protected volatile boolean inTranscation = false;
    protected volatile int transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ;
    protected volatile boolean readOnly = false;

    public boolean isInTransaction() {
        return inTranscation;
    }

    public void setAutocommit(boolean autocommit) {
        if (isInTransaction() && autocommit) {
            throw new IllegalArgumentException("is in transcation");
        }
        this.autocommit = autocommit;
    }

    public boolean isAutocommit() {
        return autocommit;
    }

    public void begin() {
        if (!isInTransaction() && !updateConnectionMap.isEmpty()) {
            throw new IllegalArgumentException("存在连接泄漏");
        }
        if (!isInTransaction()) {
            callBackBegin();
        }
        setInTranscation(true);
    }

    public void commit() {
        if (isInTransaction()) {//真正开启事务才提交
            callBackCommit();
        }
        setInTranscation(false);
        updateConnectionMap.forEach((key, value) -> value.close());
        updateConnectionMap.clear();
    }

    public void rollback() {
        if (isInTransaction()) {
            callBackRollback();
        }
        setInTranscation(false);
        updateConnectionMap.forEach((key, value) -> value.close());
        updateConnectionMap.clear();
    }

    public DefaultConnection getConnection(
            String jdbcDataSource) {
        if (!isAutocommit() && !isInTransaction()) {
            begin();
        }
        return callBackConnection(jdbcDataSource, autocommit, transactionIsolation, readOnly);
    }

    abstract protected void callBackBegin();

    abstract protected void callBackCommit();

    abstract protected void callBackRollback();

    abstract protected DefaultConnection callBackConnection(String jdbcDataSource, boolean autocommit, int transactionIsolation, boolean readOnly);

    public int getServerStatus() {
        int serverStatus = 0;
        if (isAutocommit()) {
            serverStatus |= MySQLServerStatusFlags.AUTO_COMMIT;
        }
        if (isInTransaction()) {
            serverStatus |= MySQLServerStatusFlags.IN_TRANSACTION;
        }
        return serverStatus;
    }


    public boolean isReadOnly() {
        return readOnly;
    }


    public void setReadOnly(boolean readOnly) {
        this.updateConnectionMap.forEach((key, value) -> value.setReadyOnly(readOnly));
    }

    public boolean isInTranscation() {
        return inTranscation;
    }


    private void setInTranscation(boolean inTranscation) {
        this.inTranscation = inTranscation;
    }

    public void close() {
        for (Map.Entry<String, DefaultConnection> stringDefaultConnectionEntry : updateConnectionMap.entrySet()) {
            DefaultConnection value = stringDefaultConnectionEntry.getValue();
            if (value != null) {
                value.close();
            }
        }
        updateConnectionMap.clear();
    }

    public void onEndOfResponse() {
        if (!isInTransaction()) {
            for (Map.Entry<String, DefaultConnection> stringDefaultConnectionEntry : updateConnectionMap.entrySet()) {
                DefaultConnection value = stringDefaultConnectionEntry.getValue();
                if (value != null) {
                    value.close();
                }
            }
            updateConnectionMap.clear();
        }
    }

    public int getTransactionIsolation() {
        return transactionIsolation;
    }

    public void setTransactionIsolation(int transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
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
        this.autocommit = true;
        this.inTranscation = false;
        this.transactionIsolation = Connection.TRANSACTION_REPEATABLE_READ;
        this.readOnly = false;
    }
}
package io.mycat.datasource.jdbc.transactionSession;

import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;

import java.util.HashMap;
import java.util.Map;

public abstract class TransactionSessionTemplate implements TransactionSession {
    protected final Map<String, DefaultConnection> updateConnectionMap = new HashMap<>();
   final MycatDataContext dataContext;

    public TransactionSessionTemplate(MycatDataContext dataContext) {
        this.dataContext = dataContext;
    }

    public boolean isInTransaction() {
        return dataContext.isInTransaction();
    }

    public void setAutocommit(boolean autocommit) {
        if (isInTransaction() && autocommit) {
            throw new IllegalArgumentException("is in transcation");
        }
        dataContext.setAutoCommit( autocommit);
    }

    public boolean isAutocommit() {
        return dataContext.isAutoCommit();
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

    @Override
    public <T> T getConnection(
            String jdbcDataSource) {
        if (!isAutocommit() && !isInTransaction()) {
            begin();
        }
        return(T) callBackConnection(jdbcDataSource,isAutocommit(),getTransactionIsolation() , isReadOnly());
    }

    abstract protected void callBackBegin();

    abstract protected void callBackCommit();

    abstract protected void callBackRollback();

    abstract protected DefaultConnection callBackConnection(String jdbcDataSource, boolean autocommit, int transactionIsolation, boolean readOnly);

    public int getServerStatus() {
        return dataContext.serverStatus();
    }


    public boolean isReadOnly() {
        return dataContext.isReadOnly();
    }


    public void setReadOnly(boolean readOnly) {
        this.updateConnectionMap.forEach((key, value) -> value.setReadyOnly(readOnly));
    }

    public boolean isInTranscation() {
        return dataContext.isInTransaction();
    }


    private void setInTranscation(boolean inTranscation) {
        this.dataContext.setInTransaction(inTranscation);
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
        return dataContext.getIsolation().getJdbcValue();
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
    }
}
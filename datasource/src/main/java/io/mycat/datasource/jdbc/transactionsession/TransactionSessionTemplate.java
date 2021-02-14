package io.mycat.datasource.jdbc.transactionsession;

import io.mycat.*;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.replica.DataSourceNearnessImpl;
import io.mycat.util.Dumper;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

public abstract class TransactionSessionTemplate implements TransactionSession {
    protected final Map<String, DefaultConnection> updateConnectionMap = new ConcurrentHashMap<>();
    protected final DataSourceNearness dataSourceNearness = new DataSourceNearnessImpl(this);
    protected MycatDataContext dataContext;
    protected final ConcurrentLinkedQueue<AutoCloseable> closeResourceQueue = new ConcurrentLinkedQueue<>();

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcConnectionManager.class);

    public TransactionSessionTemplate(MycatDataContext dataContext) {
        this.dataContext = dataContext;
    }

    public boolean isInTransaction() {
        return dataContext.isInTransaction();
    }

    @SneakyThrows
    public void setAutocommit(boolean autocommit) {
        for (DefaultConnection c : updateConnectionMap.values()) {
            c.getRawConnection().setAutoCommit(autocommit);
        }
        if (autocommit) {
            for (DefaultConnection value : updateConnectionMap.values()) {
                value.close();
            }
            updateConnectionMap.clear();
            setInTranscation(false);
        }
    }

    public boolean isAutocommit() {
        return dataContext.isAutocommit();
    }

    public Future<Void> begin() {
        if (!isInTransaction() && !updateConnectionMap.isEmpty()) {
            return Future.failedFuture(new IllegalArgumentException("存在连接泄漏"));
        }
        if (!isInTransaction()) {
            callBackBegin();
        }
        dataContext.setInTransaction(true);
        return Future.succeededFuture();
    }

    public Future<Void> commit() {
        try{
            if (isInTransaction() && !updateConnectionMap.isEmpty()) {//真正开启事务才提交
                callBackCommit();
            }
            setInTranscation(false);
            updateConnectionMap.forEach((key, value) -> value.close());
            updateConnectionMap.clear();
        }catch (Throwable throwable){
            return Future.failedFuture(throwable);
        }
        return Future.succeededFuture();
    }

    public Future<Void> rollback() {
        try {
            if (isInTransaction() && !updateConnectionMap.isEmpty()) {
                callBackRollback();
            }
            setInTranscation(false);
            updateConnectionMap.forEach((key, value) -> value.close());
            updateConnectionMap.clear();
        }catch (Throwable throwable){
            return Future.failedFuture(throwable);
        }
        return Future.succeededFuture();
    }

    /**
     * 模拟autocommit = 0 时候自动开启事务
     */
    public Future<Void> openStatementState() {
        Future<Void> future = closeStatenmentState();
      return   future.flatMap(unused -> {
          if (!isAutocommit()) {
              return begin();
          }
          return Future.succeededFuture();
      });
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
//        this.updateConnectionMap.forEach((key, value) -> value.setReadyOnly(readOnly));
    }


    private void setInTranscation(boolean inTranscation) {
        this.dataContext.setInTransaction(inTranscation);
    }

    public synchronized Future<Void> close() {
        Future<Void> voidFuture = closeStatenmentState();
        try {
            for (Map.Entry<String, DefaultConnection> stringDefaultConnectionEntry : updateConnectionMap.entrySet()) {
                DefaultConnection value = stringDefaultConnectionEntry.getValue();
                if (value != null) {
                    value.close();
                }
            }
            updateConnectionMap.clear();
            dataSourceNearness.clear();
        }catch (Throwable throwable){
            return (Future)CompositeFuture.all(voidFuture, Future.failedFuture(throwable));
        }
        return voidFuture;
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
    public  Future<Void> closeStatenmentState() {
        try {
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
        }catch (Throwable throwable){
            return Future.failedFuture(throwable);
        }
        return Future.succeededFuture();
    }

    public void setTransactionIsolation(int transactionIsolation) {
        this.dataContext.setIsolation(MySQLIsolation.parseJdbcValue(transactionIsolation));
        this.updateConnectionMap.forEach((key, value) -> value.setTransactionIsolation(transactionIsolation));
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


    protected Map<String, MycatConnection> callBackConnections(Set<String> jdbcDataSources,
                                                               boolean autocommit,
                                                               int transactionIsolation,
                                                               boolean readOnly) {
        if (jdbcDataSources.isEmpty()) return Collections.emptyMap();
        HashMap<String, MycatConnection> res = new HashMap<>();

        JdbcConnectionManager jdbcConnectionManager = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
        synchronized (jdbcConnectionManager) {
            for (String jdbcDataSource : jdbcDataSources) {
                DefaultConnection defaultConnection1 = updateConnectionMap.computeIfAbsent(jdbcDataSource,
                        s -> jdbcConnectionManager.getConnection(
                                jdbcDataSource,
                                autocommit,
                                transactionIsolation,
                                readOnly));
                res.put(jdbcDataSource, defaultConnection1);
            }
        }
        return res;
    }

    @Override
    public String resolveFinalTargetName(String targetName, boolean master) {
        return dataSourceNearness.getDataSourceByTargetName(targetName, master);
    }
}
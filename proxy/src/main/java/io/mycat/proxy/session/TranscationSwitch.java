package io.mycat.proxy.session;

import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.runtime.ProxyTransactionSession;
import org.apache.groovy.util.Maps;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class TranscationSwitch {
    final Map<TransactionType, Function<MycatDataContext, TransactionSession>> map;

    public TranscationSwitch() {
        this(new HashMap<>(Maps.of(TransactionType.PROXY_TRANSACTION_TYPE,
                mycatDataContext -> {
                    JdbcConnectionManager connection = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                    return new ProxyTransactionSession(connection.getDatasourceProvider().createSession(mycatDataContext));
                },
                TransactionType.JDBC_TRANSACTION_TYPE,
                mycatDataContext -> {
                    JdbcConnectionManager connection = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                    return connection.getDatasourceProvider().createSession(mycatDataContext);
                }
        )));
    }

    public TranscationSwitch(Map<TransactionType, Function<MycatDataContext, TransactionSession>> map) {
        this.map = map;
    }

    public TransactionSession ensureTranscation(MycatDataContext dataContext) {
        TransactionSession transactionSession = dataContext.getTransactionSession();
        if (transactionSession == null) {
            TransactionType transactionType = dataContext.transactionType();
            Objects.requireNonNull(transactionType);
            dataContext.setTransactionSession(transactionSession = map.get(transactionType).apply(dataContext));
        } else {
            if (!transactionSession.name().equals(dataContext.transactionType().getName())) {
                if (transactionSession.isInTransaction()) {
                    throw new IllegalArgumentException("正在处于事务状态,不能切换事务模式");
                } else {
                    //
                    Function<MycatDataContext, TransactionSession> transactionSessionFunction =
                            Objects.requireNonNull(map.get(dataContext.transactionType()));
                    TransactionSession newTransactionSession = transactionSessionFunction.apply(dataContext);

                    setTranscation(dataContext, transactionSession, newTransactionSession);
                    transactionSession = newTransactionSession;
                }
            }
        }
        return transactionSession;
    }

    private void setTranscation(MycatDataContext container, TransactionSession transactionSession, TransactionSession newTransactionSession) {
        newTransactionSession.setReadOnly(transactionSession.isReadOnly());
        newTransactionSession.setAutocommit(transactionSession.isAutocommit());
        newTransactionSession.setTransactionIsolation(transactionSession.getTransactionIsolation());

        container.setTransactionSession(newTransactionSession);
    }

    public void banProxy() {
        map.put(TransactionType.PROXY_TRANSACTION_TYPE,map.get(TransactionType.JDBC_TRANSACTION_TYPE));
    }
}

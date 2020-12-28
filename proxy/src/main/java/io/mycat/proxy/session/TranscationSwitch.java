package io.mycat.proxy.session;

import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.datasource.jdbc.datasource.JdbcConnectionManager;
import io.mycat.runtime.ProxyTransactionSession;
import org.apache.groovy.util.Maps;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

public class TranscationSwitch {
    final Map<TransactionType, Function<MycatDataContext, TransactionSession>> map;

    public TranscationSwitch() {
        this(Maps.of(TransactionType.PROXY_TRANSACTION_TYPE,
                mycatDataContext -> {
                    JdbcConnectionManager connection = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                    return new ProxyTransactionSession(connection.getDatasourceProvider().createSession(mycatDataContext));
                },
                TransactionType.JDBC_TRANSACTION_TYPE,
                mycatDataContext -> {
                    JdbcConnectionManager connection = MetaClusterCurrent.wrapper(JdbcConnectionManager.class);
                    return connection.getDatasourceProvider().createSession(mycatDataContext);
                }
        ));
    }

    public TranscationSwitch(Map<TransactionType, Function<MycatDataContext, TransactionSession>> map) {
        this.map = map;
    }

    public TransactionSession ensureTranscation(MycatDataContext container) {
        TransactionSession transactionSession = container.getTransactionSession();
        if (transactionSession == null) {
            TransactionType transactionType = container.transactionType();
            Objects.requireNonNull(transactionType);
            container.setTransactionSession(transactionSession = map.get(transactionType).apply(container));
        } else {
            if (!transactionSession.name().equals(container.transactionType().getName())) {
                if (transactionSession.isInTransaction()) {
                    throw new IllegalArgumentException("正在处于事务状态,不能切换事务模式");
                } else {
                    //
                    Function<MycatDataContext, TransactionSession> transactionSessionFunction =
                            Objects.requireNonNull(map.get(container.transactionType()));
                    TransactionSession newTransactionSession = transactionSessionFunction.apply(container);

                    newTransactionSession.setReadOnly(transactionSession.isReadOnly());
                    newTransactionSession.setAutocommit(transactionSession.isAutocommit());
                    newTransactionSession.setTransactionIsolation(transactionSession.getTransactionIsolation());

                    container.setTransactionSession(newTransactionSession);
                    transactionSession = newTransactionSession;
                }
            }
        }
        return transactionSession;
    }
}

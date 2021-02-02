package io.mycat.proxy.session;

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.XaLog;
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
                    XaLog xaLog = MetaClusterCurrent.wrapper(XaLog.class);
                    return new ProxyTransactionSession(()->MetaClusterCurrent.wrapper(MySQLManager.class),xaLog,connection.getDatasourceProvider().createSession(mycatDataContext));
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
            XaLog xaLog = MetaClusterCurrent.wrapper(XaLog.class);
               dataContext.setTransactionSession(transactionSession =new ProxyTransactionSession(()->MetaClusterCurrent.wrapper(MySQLManager.class),xaLog,map.get(TransactionType.PROXY_TRANSACTION_TYPE).apply(dataContext)));
        } else {

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

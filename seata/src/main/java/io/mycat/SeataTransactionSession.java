package io.mycat;

import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.transactionsession.LocalTransactionSession;
import io.seata.core.context.RootContext;
import io.seata.core.model.GlobalStatus;
import io.seata.tm.api.GlobalTransaction;
import io.seata.tm.api.GlobalTransactionContext;
import io.seata.tm.api.transaction.SuspendedResourcesHolder;
import lombok.SneakyThrows;

import java.util.Map;

public class SeataTransactionSession extends LocalTransactionSession {
    GlobalTransaction tx;
    private SuspendedResourcesHolder holder = null;

    public SeataTransactionSession(MycatDataContext dataContext) {
        super(dataContext);
    }

    @Override
    @SneakyThrows
    public void openStatementState() {
        if (holder != null && tx != null) {
            tx.resume(holder);
        }
        super.openStatementState();
    }

    @Override
    @SneakyThrows
    public void begin() {
        if (tx == null) {
            tx = GlobalTransactionContext.createNew();
        }
        String xid = RootContext.getXID();
        if (xid == null){
            GlobalStatus localStatus = tx.getLocalStatus();
                switch (localStatus) {
                    case Rollbacked:
                    case Committed:
                    case Finished:
                    case UnKnown:
                        for (Map.Entry<String, DefaultConnection> e : updateConnectionMap.entrySet()) {
                            e.getValue().close();
                        }
                        updateConnectionMap.clear();
                        tx.begin();
                        mycatXid = tx.getXid();

                        break;
                    case Begin:
                    case Committing:
                    case CommitRetrying:
                    case Rollbacking:
                    case RollbackRetrying:
                    case TimeoutRollbacking:
                    case TimeoutRollbackRetrying:
                    case AsyncCommitting:
                    case CommitFailed:
                    case RollbackFailed:
                    case TimeoutRollbacked:
                    case TimeoutRollbackFailed:
                    default:
                }
        }
        dataContext.setInTransaction(true);
    }


    @Override
    @SneakyThrows
    public void commit() {
        for (Map.Entry<String, DefaultConnection> e : updateConnectionMap.entrySet()) {
            DefaultConnection value = e.getValue();
            value.getRawConnection().commit();
        }
        tx.commit();
        for (Map.Entry<String, DefaultConnection> e : updateConnectionMap.entrySet()) {
            DefaultConnection value = e.getValue();
            value.close();
        }
        updateConnectionMap.clear();
        tx = null;
        dataContext.setInTransaction(false);
    }

    @Override
    @SneakyThrows
    public void rollback() {
        for (Map.Entry<String, DefaultConnection> e : updateConnectionMap.entrySet()) {
            e.getValue().getRawConnection().rollback();
        }
        tx.rollback();
        for (Map.Entry<String, DefaultConnection> e : updateConnectionMap.entrySet()) {
            e.getValue().close();
        }
        updateConnectionMap.clear();
        tx = null;
        dataContext.setInTransaction(false);
    }

    @Override
    public String getTxId() {
        return tx.getXid();
    }

    @Override
    @SneakyThrows
    public void closeStatenmentState() {
        super.closeStatenmentState();
        if (tx != null) {
            this.holder = tx.suspend();
        }

    }
}

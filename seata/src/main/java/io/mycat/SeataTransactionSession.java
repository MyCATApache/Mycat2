package io.mycat;

import io.mycat.datasource.jdbc.transactionsession.LocalTransactionSession;
import io.seata.core.context.RootContext;
import io.seata.tm.api.GlobalTransaction;
import io.seata.tm.api.GlobalTransactionContext;
import lombok.SneakyThrows;

public class SeataTransactionSession extends LocalTransactionSession {
    GlobalTransaction tx;

    public SeataTransactionSession(MycatDataContext dataContext) {
        super(dataContext);
    }

    @Override
    public void ensureTranscation() {
        super.ensureTranscation();
        if (mycatXid != null) {
            RootContext.bind(mycatXid);
        }
    }

    @Override
    @SneakyThrows
    public void begin() {
        if (tx != null) {
            tx = GlobalTransactionContext.createNew();
            tx.begin();
            mycatXid = tx.getXid();
            dataContext.setInTransaction(true);
        }
    }


    @Override
    @SneakyThrows
    public void commit() {
        if (tx != null) {
            tx.commit();
            dataContext.setInTransaction(false);
        }
        super.commit();
        mycatXid = null;
    }

    @Override
    @SneakyThrows
    public void rollback() {
        if (tx != null) {
            tx.rollback();
            dataContext.setInTransaction(false);
        }
        super.rollback();
        mycatXid = null;
    }

    @Override
    public String getTxId() {
        return mycatXid;
    }

    @Override
    public void clearJdbcConnection() {
        super.clearJdbcConnection();
        RootContext.unbind();
    }
}

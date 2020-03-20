package io.mycat.proxy.session;

import io.mycat.MycatDataContext;
import io.mycat.TransactionSessionRunner;

public class SimpleTransactionSessionRunner implements TransactionSessionRunner {
    @Override
    public void run(MycatDataContext dataContext, Runnable runnable) {
        runnable.run();
    }

    @Override
    public void block(MycatDataContext mycatDataContext, Runnable runnable) {
        runnable.run();
    }
}
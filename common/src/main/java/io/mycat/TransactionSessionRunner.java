package io.mycat;

public interface TransactionSessionRunner {
    void run(MycatDataContext dataContext, Runnable runnable);
}
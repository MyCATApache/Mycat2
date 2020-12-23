package io.mycat;

/**
 * @author Junwen Chen
 **/
public interface TransactionSessionRunner {
    void run(MycatDataContext dataContext, Runnable runnable);

    void block(MycatDataContext mycatDataContext, Runnable runnable);
}
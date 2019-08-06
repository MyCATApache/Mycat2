package io.mycat.datasource.jdbc.transaction;

import io.mycat.datasource.jdbc.GridRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.session.MycatSession;
import java.util.Collection;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public class TransactionProcessUnitManager {

  final ConcurrentMap<MycatSession, TransactionProcessUnit> map = new ConcurrentHashMap<>();
  final ConcurrentLinkedQueue<TransactionProcessUnit> idleList = new ConcurrentLinkedQueue<>();
  final Collection<TransactionProcessUnit> allSession = new ConcurrentLinkedQueue<>();
  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(TransactionProcessUnitManager.class);
  private final GridRuntime runtime;

  public TransactionProcessUnitManager(GridRuntime runtime) {
    this.runtime = runtime;
  }

  private long timeout;

  void checkTimeout() {
    long now = System.currentTimeMillis();
    map.forEach((mycat, thread) -> {
      String message = "timeout";
      if (thread.getStartTime() + timeout > now) {

      }
    });
  }

  public void run(MycatSession session, Runnable runnable) {
    TransactionProcessUnit transactionThread = map.compute(session,
        (session1, transactionThread1) -> {
          if (transactionThread1 == null) {
            transactionThread1 = idleList.poll();
            if (transactionThread1 == null) {
              transactionThread1 = new TransactionProcessUnit(runtime);
              transactionThread1.start();
              allSession.add(transactionThread1);
            }
          }
          Objects.requireNonNull(transactionThread1);
          return transactionThread1;
        });
    transactionThread.run(runnable);
    recycleTransactionThread(session, transactionThread);
  }

  public void recycleTransactionThread(
      MycatSession session, TransactionProcessUnit transactionThread) {
    transactionThread.getTransactionSession().afterDoAction();
    if (!transactionThread.getTransactionSession().isInTransaction()) {
      TransactionProcessUnit remove = map.remove(session);
      if (!idleList.offer(remove)) {
        //remove.close();
        allSession.remove(remove);
      }
    }
  }


}
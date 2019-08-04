package io.mycat.datasource.jdbc.transaction;

import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.session.MycatSession;
import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;

public enum TransactionProcessUnitManager {
  INSTANCE;
  final ConcurrentMap<MycatSession, TransactionProcessUnit> map = new ConcurrentHashMap<>();
  final ConcurrentLinkedQueue<TransactionProcessUnit> idleList = new ConcurrentLinkedQueue<>();
  final Collection<TransactionProcessUnit> allSession = new ConcurrentLinkedQueue<>();
  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(TransactionProcessUnitManager.class);
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
              transactionThread1 = new TransactionProcessUnit();
              transactionThread1.start();
              allSession.add(transactionThread1);
            }
          }
          return transactionThread1;
        });
    transactionThread.run(runnable);
    recycleTransactionThread(session);
  }

  public void recycleTransactionThread(MycatSession session) {
    if (!session.isInTransaction()) {
      TransactionProcessUnit remove = map.remove(session);
      if(!idleList.offer(remove)){
        remove.close();
        allSession.remove(remove);
      }
    }
  }


}
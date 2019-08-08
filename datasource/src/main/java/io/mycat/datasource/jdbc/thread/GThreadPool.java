package io.mycat.datasource.jdbc.thread;

import io.mycat.bindThread.BindThread;
import io.mycat.bindThread.BindThreadKey;
import io.mycat.bindThread.BindThreadPool;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.util.concurrent.TimeUnit;

public class GThreadPool<KEY extends BindThreadKey> extends BindThreadPool<KEY, BindThread> {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(GThreadPool.class);
  static int maxPengdingLimit = -1;
  static int waitTaskTimeout = 5;
  static TimeUnit timeUnit = TimeUnit.SECONDS;
  static int minThread = 2;
  static int maxThread = 50;

  public GThreadPool(GRuntime runtime) {
    super(maxPengdingLimit, waitTaskTimeout, timeUnit, minThread, maxThread,
        bindThreadPool -> new GThread(runtime, bindThreadPool), (e) -> LOGGER.error("", e));
  }

}
package io.mycat.router.sequence;

import io.mycat.MycatException;
import io.mycat.beans.mysql.packet.ErrorPacket;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.mysqlapi.MySQLAPI;
import io.mycat.mysqlapi.MySQLAPIRuntime;
import io.mycat.mysqlapi.callback.MySQLAPIExceptionCallback;
import io.mycat.mysqlapi.callback.MySQLAPISessionCallback;
import io.mycat.mysqlapi.callback.MySQLJobCallback;
import io.mycat.mysqlapi.collector.OneResultSetCollector;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MySQLSequenceHandlerImpl implements SequenceHandler {

  private static final MycatLogger LOGGER = MycatLoggerFactory
      .getLogger(MySQLSequenceHandlerImpl.class);
  private final AtomicBoolean fetch = new AtomicBoolean(false);
  private final String dataSourceName;
  private MySQLAPIRuntime mySQLAPIRuntime;
  private volatile AtomicLong currentValue = null;
  private volatile long maxValue = Long.MIN_VALUE;
  private String fetchSQL;
  private long timeout;

  public MySQLSequenceHandlerImpl(MySQLAPIRuntime mySQLAPIRuntime, String dataSourceName,
      String fetchSQL, long timeout) {
    this.mySQLAPIRuntime = mySQLAPIRuntime;
    this.dataSourceName = dataSourceName;
    this.fetchSQL = fetchSQL;
    this.timeout = timeout;
  }

  public void nextId(SequenceCallback callback) {
    if (currentValue == null) {
      if (fetch.compareAndSet(false, true)) {
        updateSeqFromDb(callback);
        return;
      } else {
        pending(callback);
        return;
      }
    } else {
      long value = currentValue.incrementAndGet();
      if (value < maxValue) {
        callback.onSequence(value);
        return;
      } else {
        currentValue = null;
        nextId(callback);
      }
    }
  }

  public void pending(SequenceCallback callback) {
    mySQLAPIRuntime.addPengdingJob(new MySQLJobCallback() {
      long startTime = System.currentTimeMillis();

      @Override
      public void run() throws Exception {
        if (System.currentTimeMillis() - startTime > timeout) {
          stop(new Exception("seq timeout"));
        } else {
          nextId(callback);
        }
      }

      @Override
      public void stop(Exception reason) {
        callback.onException(reason);
      }

      @Override
      public String message() {
        return "SequenceModifierImpl";
      }
    });
  }

  public void updateSeqFromDb(SequenceCallback callback) {
    mySQLAPIRuntime.create(dataSourceName, new
        MySQLAPISessionCallback() {
          @Override
          public void onSession(MySQLAPI mySQLAPI) {
            OneResultSetCollector collector = new OneResultSetCollector();
            mySQLAPI.query(fetchSQL, collector, new MySQLAPIExceptionCallback() {

              @Override
              public void onException(Exception exception, MySQLAPI mySQLAPI) {
                fetch.set(false);
                callback.onException(exception);
              }

              @Override
              public void onFinished(boolean monopolize, MySQLAPI mySQLAPI) {
                try {
                  mySQLAPI.close();
                } catch (Exception e) {
                  LOGGER.error("", e);
                }
                try {
                  Iterator<Object[]> iterator = collector.iterator();
                  Object[] next = iterator.next();
                  Long currentValue = (Long) next[1];
                  Long increment = (Long) next[2];
                  MySQLSequenceHandlerImpl.this.currentValue = new AtomicLong(currentValue);
                  MySQLSequenceHandlerImpl.this.maxValue = currentValue + increment;
                } catch (Exception e) {
                  callback.onException(e);
                  return;
                } finally {
                  fetch.set(false);
                }
                nextId(callback);
              }

              @Override
              public void onErrorPacket(ErrorPacket errorPacket, boolean monopolize,
                  MySQLAPI mySQLAPI) {
                fetch.set(false);
                try {
                  mySQLAPI.close();
                } catch (Exception e) {
                  LOGGER.error("", e);
                }
                callback.onException(new MycatException(errorPacket.getErrorMessageString()));
              }
            });
          }

          @Override
          public void onException(Exception exception) {
            fetch.set(false);
            callback.onException(exception);
          }
        });
  }
}
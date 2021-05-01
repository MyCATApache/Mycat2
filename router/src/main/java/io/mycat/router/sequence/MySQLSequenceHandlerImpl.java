/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router.sequence;

import io.mycat.MycatException;
import io.mycat.api.MySQLAPI;
import io.mycat.api.MySQLAPIRuntime;
import io.mycat.api.callback.MySQLAPIExceptionCallback;
import io.mycat.api.callback.MySQLAPISessionCallback;
import io.mycat.api.callback.MySQLJobCallback;
import io.mycat.api.collector.OneResultSetCollector;
import io.mycat.beans.mysql.packet.ErrorPacket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class MySQLSequenceHandlerImpl implements SequenceHandler<MySQLAPIRuntime> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MySQLSequenceHandlerImpl.class);
  private MySQLAPIRuntime mySQLAPIRuntime;
  private long timeout;
  private final Map<SeqInfoKey, SeqInfoValue> map = new HashMap<>();

  @Override
  public void nextId(String schema, String seqName, SequenceCallback callback) {
    try {
      SeqInfoValue seqInfoValue = getSeqInfoValue(schema, seqName);
      AtomicLong valueBox = seqInfoValue.getValueBox();
      if (valueBox == null) {
        if (seqInfoValue.getFetchLock().compareAndSet(false, true)) {
          updateSeqFromDb(seqInfoValue, callback);
          return;
        } else {
          pending(schema, seqName, callback);
          return;
        }
      } else {
        long cur = valueBox.get();
        long inc = seqInfoValue.tryIncrement();
        if (inc > cur) {
          callback.onSequence(inc);
          return;
        } else {
          seqInfoValue.setValueBox(null);
          nextId(schema, seqName, callback);
          return;
        }
      }
    } catch (Exception e) {
      LOGGER.error("", e);
      callback.onException(e);
    }
  }

  private void updateSeqFromDb(SeqInfoValue seqInfoValue,
      SequenceCallback callback) {
    String dataSourceName = seqInfoValue.getDataSourceName();
    mySQLAPIRuntime.create(dataSourceName, new
        MySQLAPISessionCallback() {
          @Override
          public void onSession(MySQLAPI mySQLAPI) {
            OneResultSetCollector collector = new OneResultSetCollector();
            String sql = seqInfoValue.getSql();
            mySQLAPI.query(sql, collector,
                new MySQLAPIExceptionCallback() {

                  @Override
                  public void onException(Exception exception, MySQLAPI mySQLAPI) {
                    seqInfoValue.getFetchLock().set(false);
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
                      String seqText = (String) iterator.next()[0];
                      String[] values = seqText.split(",");
                      long currentValue = Long.parseLong(values[0]);
                      long increment = Long.parseLong(values[1]);
                      seqInfoValue.setValueBox(new AtomicLong(currentValue));
                      seqInfoValue.setMaxValue(currentValue + increment);
                    } catch (Exception e) {
                      callback.onException(e);
                      return;
                    } finally {
                      seqInfoValue.getFetchLock().set(false);
                    }
                    SeqInfoKey seqInfoKey = seqInfoValue.getSeqInfoKey();
                    nextId(seqInfoKey.getSchema(), seqInfoKey.getTable(), callback);
                  }

                  @Override
                  public void onErrorPacket(ErrorPacket errorPacket, boolean monopolize,
                      MySQLAPI mySQLAPI) {
                    seqInfoValue.getFetchLock().set(false);
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
            seqInfoValue.getFetchLock().set(false);
            callback.onException(exception);
          }
        });
  }


  @Override
  public void init(MySQLAPIRuntime mySQLAPIRuntime,
      Map<String, String> properties) {
    Objects.requireNonNull(mySQLAPIRuntime);
    this.mySQLAPIRuntime = mySQLAPIRuntime;

    for (Entry<String, String> entry : properties.entrySet()) {
      String key = entry.getKey();
      if (key.startsWith("mysqlSeqSource-")) {
        init(key, entry.getValue());
      }
    }

    this.timeout = Long.parseLong(
        properties.getOrDefault("mysqlSeqTimeout", Long.toString(TimeUnit.SECONDS.toMillis(1))));

  }

  public void pending(String schema, String table, SequenceCallback callback) {
    mySQLAPIRuntime.addPengdingJob(new MySQLJobCallback() {
      long startTime = System.currentTimeMillis();

      @Override
      public void run() throws Exception {
        if (System.currentTimeMillis() - startTime > MySQLSequenceHandlerImpl.this.timeout) {
          stop(new Exception("seq timeout"));
        } else {
          nextId(schema, table, callback);
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


  private void init(String key, String value) {
    if (key != null && key.startsWith("mysqlSeqSource-")) {
      String[] keys = key.split("-");
      if (keys.length != 3) {
        throw new MycatException("error format:{}", key);
      }
      SeqInfoKey seqInfoKey = new SeqInfoKey(keys[1], keys[2]);
      String[] values = value.split("-");
      if (values.length != 3) {
        throw new MycatException("error format:{}", key);
      }
      SeqInfoValue seqInfoValue = new SeqInfoValue(seqInfoKey, values[0], values[1], values[2]);
      map.put(seqInfoKey, seqInfoValue);
    }
  }

  private SeqInfoValue getSeqInfoValue(String schema, String seqName) {
    SeqInfoValue seqInfoValue = map.get(new SeqInfoKey(schema, seqName));
    if (seqInfoValue == null) {
      throw new MycatException("seq name/table :{}   not found", seqName);
    }
    return seqInfoValue;
  }

  static class SeqInfoKey {

    private final String schema;
    private final String table;

    public SeqInfoKey(String schema, String table) {
      this.schema = schema;
      this.table = table;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      SeqInfoKey that = (SeqInfoKey) o;

      if (schema != null ? !schema.equals(that.schema) : that.schema != null) {
        return false;
      }
      return table != null ? table.equals(that.table) : that.table == null;
    }

    @Override
    public int hashCode() {
      int result = schema != null ? schema.hashCode() : 0;
      result = 31 * result + (table != null ? table.hashCode() : 0);
      return result;
    }

    public String getSchema() {
      return schema;
    }

    public String getTable() {
      return table;
    }
  }

  static class SeqInfoValue {

    private final SeqInfoKey seqInfoKey;
    private final String dataSourceName;
    private final String sql;

    private final AtomicBoolean fetch = new AtomicBoolean(false);
    private volatile AtomicLong currentValue = null;
    private volatile long maxValue = Long.MIN_VALUE;

    public SeqInfoValue(SeqInfoKey seqInfoKey, String dataSourceName, String sequenceSchema,
        String sequenceName) {
      this.seqInfoKey = seqInfoKey;
      this.dataSourceName = dataSourceName;
      this.sql = String
          .format("SELECT %s.mycat_seq_nextval('%s');", sequenceSchema, sequenceName);
    }

    public long tryIncrement() {
      return currentValue.updateAndGet(this::applyAsLong);
    }

    private long applyAsLong(long operand) {
      if (operand < maxValue) {
        return ++operand;
      } else {
        return operand;
      }
    }

    public String getDataSourceName() {
      return dataSourceName;
    }

    public String getSql() {
      return sql;
    }

    public AtomicBoolean getFetchLock() {
      return fetch;
    }

    public AtomicLong getValueBox() {
      return currentValue;
    }

    public void setValueBox(AtomicLong currentValue) {
      this.currentValue = currentValue;
    }

    public long getMaxValue() {
      return maxValue;
    }

    public void setMaxValue(long l) {
      this.maxValue = l;
    }

    public SeqInfoKey getSeqInfoKey() {
      return seqInfoKey;
    }
  }
}
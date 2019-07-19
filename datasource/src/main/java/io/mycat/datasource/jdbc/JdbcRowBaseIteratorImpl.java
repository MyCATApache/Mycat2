package io.mycat.datasource.jdbc;

import io.mycat.MycatException;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.mysqlapi.collector.RowBaseIterator;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.io.InputStream;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

public class JdbcRowBaseIteratorImpl implements RowBaseIterator {

  final static MycatLogger LOGGER = MycatLoggerFactory.getLogger(JdbcRowBaseIteratorImpl.class);

  private final Statement statement;
  private final ResultSet resultSet;

  public JdbcRowBaseIteratorImpl(Statement statement, ResultSet resultSet) {
    this.statement = statement;
    this.resultSet = resultSet;
  }


  private String toMessage(Exception e) {
    return e.toString();
  }

  @Override
  public MycatRowMetaData metaData() {
    try {
      return new JdbcRowMetaDataImpl(resultSet.getMetaData());
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public boolean next() {
    try {
      return resultSet.next();
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public void close() {
    try {
      resultSet.close();
    } catch (Exception e) {
      LOGGER.error("", e);
    }
    try {
      statement.close();
    } catch (Exception e) {
      LOGGER.error("", e);
    }
  }

  @Override
  public boolean wasNull() {
    try {
      return resultSet.wasNull();
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public String getString(int columnIndex) {
    try {
      return resultSet.getString(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public boolean getBoolean(int columnIndex) {
    try {
      return resultSet.getBoolean(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public byte getByte(int columnIndex) {
    try {
      return resultSet.getByte(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public short getShort(int columnIndex) {
    try {
      return resultSet.getShort(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public int getInt(int columnIndex) {
    try {
      return resultSet.getInt(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public long getLong(int columnIndex) {
    try {
      return resultSet.getLong(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public float getFloat(int columnIndex) {
    try {
      return resultSet.getFloat(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public double getDouble(int columnIndex) {
    try {
      return resultSet.getDouble(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public byte[] getBytes(int columnIndex) {
    try {
      return resultSet.getBytes(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public Date getDate(int columnIndex) {
    try {
      return resultSet.getDate(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public Time getTime(int columnIndex) {
    try {
      return resultSet.getTime(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public Timestamp getTimestamp(int columnIndex) {
    try {
      return resultSet.getTimestamp(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) {
    try {
      return resultSet.getAsciiStream(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) {
    try {
      return resultSet.getBinaryStream(columnIndex);
    } catch (Exception e) {
      throw new MycatException(toMessage(e));
    }
  }
}
/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.packet;

import java.math.BigDecimal;
import java.sql.Date;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 文本结果集收集类
 *
 * @author jamie12221
 * @date 2019-05-10 13:21
 */
public abstract class TextResultSetTransforCollector implements ResultSetCollector {

  static final Logger logger = LoggerFactory.getLogger(TextResultSetTransforCollector.class);
  static final boolean log = false;

  @Override
  public void onResultSetStart() {
    if (log) {
      logger.debug("onResultSetStart");
    }
  }

  @Override
  public void onResultSetEnd() {
    if (log) {
      logger.debug("onResultSetEnd");
    }
  }


  protected void addValue(int columnIndex) {

  }

  protected void addValue(int columnIndex, String value) {

  }


  protected void addValue(int columnIndex, long value) {

  }

  protected void addValue(int columnIndex, double value) {

  }


  protected void addValue(int columnIndex, byte[] value) {

  }

  protected void addValue(int columnIndex, byte value) {

  }

  protected void addValue(int columnIndex, BigDecimal value) {

  }

  @Override
  public void onRowStart() {
    if (log)
      logger.debug("onRowStart");
  }

  @Override
  public void onRowEnd() {
    if (log)
      logger.debug("onRowEnd");
  }

  @Override
  public void collectDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale,
      MySQLPacket mySQLPacket, int startIndex) {
    BigDecimal bigDecimal = new BigDecimal(mySQLPacket.readLenencString());
    addValue(columnIndex, bigDecimal);
    if (log) {
      logger.debug("{}:{}", columnDef.getColumnNameString(), bigDecimal);
    }
  }

  @Override
  public void collectTiny(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      int i = Integer.parseInt(mySQLPacket.readLenencString());
      addValue(columnIndex, i);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), i);
      }
    }
  }

  @Override
  public void collectGeometry(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      String v = mySQLPacket.readLenencString();
      addValue(columnIndex, v);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), v);
      }
    }
  }

  @Override
  public void collectTinyString(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      String lenencBytes = mySQLPacket.readLenencString();
      addValue(columnIndex, lenencBytes);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencBytes);
      }
    }
  }

  @Override
  public void collectVarString(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      String lenencBytes = mySQLPacket.readLenencString();
      addValue(columnIndex, lenencBytes);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencBytes);
      }
    }
  }

  @Override
  public void collectShort(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      int lenencInt = Integer.parseInt(mySQLPacket.readLenencString());
      addValue(columnIndex, lenencInt);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencInt);
      }
    }
  }

  @Override
  public void collectBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      byte[] lenencBytes = mySQLPacket.getLenencBytes(startIndex);
      addValue(columnIndex, lenencBytes);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencBytes);
      }
    }
  }

  @Override
  public void collectMediumBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      byte[] lenencBytes = mySQLPacket.getLenencBytes(startIndex);
      addValue(columnIndex, lenencBytes);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencBytes);
      }
    }
  }

  @Override
  public void collectTinyBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      byte[] lenencBytes = mySQLPacket.getLenencBytes(startIndex);
      addValue(columnIndex, lenencBytes);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencBytes);
      }
    }
  }

  @Override
  public void collectFloat(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      double v = Double.parseDouble(mySQLPacket.readLenencString());
      addValue(columnIndex, v);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), v);
      }
    }
  }

  @Override
  public void collectDouble(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      double v = Double.parseDouble(mySQLPacket.readLenencString());
      addValue(columnIndex, v);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), v);
      }
    }
  }

  @Override
  public void collectNull(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      addValue(columnIndex);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), null);
      }
    }
  }

  @Override
  public void collectTimestamp(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      Date date = Date.valueOf(mySQLPacket.readLenencString());
      addValue(columnIndex, date);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), date);
      }
    }
  }

  private void addValue(int columnIndex, Date date) {

  }

  @Override
  public void collectInt24(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      int lenencInt = Integer.parseInt(mySQLPacket.readLenencString());
      addValue(columnIndex, lenencInt);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencInt);
      }
    }
  }

  @Override
  public void collectDate(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      Date date = Date.valueOf(mySQLPacket.readLenencString());
      addValue(columnIndex, date);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), date);
      }
    }
  }

  @Override
  public void collectTime(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      Date date = Date.valueOf(mySQLPacket.readLenencString());
      addValue(columnIndex, date);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), date);
      }
    }
  }

  @Override
  public void collectDatetime(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      Date date = Date.valueOf(mySQLPacket.readLenencString());
      addValue(columnIndex, date);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), date);
      }
    }
  }

  @Override
  public void collectYear(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      Date date = Date.valueOf(mySQLPacket.readLenencString());
      addValue(columnIndex, date);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), date);
      }
    }
  }

  @Override
  public void collectNewDate(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      Date date = Date.valueOf(mySQLPacket.readLenencString());
      addValue(columnIndex, date);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), date);
      }
    }
  }

  @Override
  public void collectVarChar(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      String lenencString = mySQLPacket.readLenencString();
      addValue(columnIndex, lenencString);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencString);
      }
    }
  }

  @Override
  public void collectBit(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      String lenencString = mySQLPacket.readLenencString();
      addValue(columnIndex, lenencString);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencString);
      }
    }
  }

  @Override
  public void collectNewDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale,
      MySQLPacket mySQLPacket, int startIndex) {
    if (true) {
      BigDecimal bigDecimal = new BigDecimal(mySQLPacket.readLenencString());
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), bigDecimal);
      }
    }
  }

  @Override
  public void collectEnum(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      String lenencString = mySQLPacket.readLenencString();
      addValue(columnIndex, lenencString);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencString);
      }
    }
  }

  @Override
  public void collectSet(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      String lenencString = mySQLPacket.readLenencString();
      addValue(columnIndex, lenencString);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencString);
      }
    }
  }

  @Override
  public void collectLong(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      long lenencInt = Long.parseLong(mySQLPacket.readLenencString());
      addValue(columnIndex, lenencInt);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencInt);
      }
    }
  }

  @Override
  public void collectLongLong(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      long lenencInt = Long.parseLong(mySQLPacket.readLenencString());
      addValue(columnIndex, lenencInt);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencInt);
      }
    }
  }

  @Override
  public void collectLongBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      byte[] lenencBytes = mySQLPacket.getLenencBytes(startIndex);
      addValue(columnIndex, lenencBytes);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), lenencBytes);
      }
    }
  }

  @Override
  public void collectColumnList(ColumnDefPacket[] packets) {

  }
}

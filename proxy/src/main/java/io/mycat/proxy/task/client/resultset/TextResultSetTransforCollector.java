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
package io.mycat.proxy.task.client.resultset;

import io.mycat.beans.mysql.packet.ColumnDefPacket;
import io.mycat.proxy.packet.MySQLPacket;
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
public interface TextResultSetTransforCollector extends ResultSetTransfor {

  Logger logger = LoggerFactory.getLogger(TextResultSetTransforCollector.class);
  boolean log = false;

  @Override
  default void onResultSetStart() {
    if (log) {
      logger.debug("onResultSetStart");
    }
  }

  @Override
  default void onResultSetEnd() {
    if (log) {
      logger.debug("onResultSetEnd");
    }
  }


  default void addValue(int columnIndex) {

  }

  default void addValue(int columnIndex, String value) {

  }


  default void addValue(int columnIndex, long value) {

  }

  default void addValue(int columnIndex, double value) {

  }


  default void addValue(int columnIndex, byte[] value) {

  }

  default void addValue(int columnIndex, byte value) {

  }

  default void addValue(int columnIndex, BigDecimal value) {

  }

  @Override
  default void onRowStart() {
    if (log)
      logger.debug("onRowStart");
  }

  @Override
  default void onRowEnd() {
    if (log)
      logger.debug("onRowEnd");
  }

  @Override
  default void collectDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale,
      MySQLPacket mySQLPacket, int startIndex) {
    BigDecimal bigDecimal = new BigDecimal(mySQLPacket.readLenencString());
    addValue(columnIndex, bigDecimal);
    if (log) {
      logger.debug("{}:{}", columnDef.getColumnNameString(), bigDecimal);
    }
  }

  @Override
  default void collectTiny(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectGeometry(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectTinyString(int columnIndex, ColumnDefPacket columnDef,
      MySQLPacket mySQLPacket,
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
  default void collectVarString(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectShort(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectMediumBlob(int columnIndex, ColumnDefPacket columnDef,
      MySQLPacket mySQLPacket,
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
  default void collectTinyBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectFloat(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectDouble(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectNull(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      addValue(columnIndex);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), null);
      }
    }
  }

  @Override
  default void collectTimestamp(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
      int startIndex) {
    if (true) {
      Date date = Date.valueOf(mySQLPacket.readLenencString());
      addValue(columnIndex, date);
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), date);
      }
    }
  }

  default void addValue(int columnIndex, Date date) {

  }

  @Override
  default void collectInt24(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectDate(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectTime(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectDatetime(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectYear(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectNewDate(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectVarChar(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectBit(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectNewDecimal(int columnIndex, ColumnDefPacket columnDef, int decimalScale,
      MySQLPacket mySQLPacket, int startIndex) {
    if (true) {
      BigDecimal bigDecimal = new BigDecimal(mySQLPacket.readLenencString());
      if (log) {
        logger.debug("{}:{}", columnDef.getColumnNameString(), bigDecimal);
      }
    }
  }

  @Override
  default void collectEnum(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectSet(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectLong(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectLongLong(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectLongBlob(int columnIndex, ColumnDefPacket columnDef, MySQLPacket mySQLPacket,
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
  default void collectColumnList(ColumnDefPacket[] packets) {

  }
}

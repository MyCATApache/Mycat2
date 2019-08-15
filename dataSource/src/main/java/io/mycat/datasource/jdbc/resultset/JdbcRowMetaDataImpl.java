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
package io.mycat.datasource.jdbc.resultset;

import io.mycat.MycatException;
import io.mycat.beans.mycat.MycatRowMetaData;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class JdbcRowMetaDataImpl implements MycatRowMetaData {

  final ResultSetMetaData resultSetMetaData;

  public JdbcRowMetaDataImpl(ResultSetMetaData resultSetMetaData) {
    this.resultSetMetaData = resultSetMetaData;
  }

  private String toMessage(Exception e) {
    return e.toString();
  }

  public int getColumnCount() {
    try {
      return resultSetMetaData.getColumnCount();
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public boolean isAutoIncrement(int column) {
    try {
      return resultSetMetaData.isAutoIncrement(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public boolean isCaseSensitive(int column) {
    try {
      return resultSetMetaData.isCaseSensitive(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public int isNullable(int column) {
    try {
      return resultSetMetaData.isNullable(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public boolean isSigned(int column) {
    try {
      return resultSetMetaData.isSigned(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public int getColumnDisplaySize(int column) {
    try {
      return resultSetMetaData.getColumnDisplaySize(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public String getColumnName(int column) {
    try {
      return resultSetMetaData.getColumnName(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public String getSchemaName(int column) {
    try {
      return resultSetMetaData.getSchemaName(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public int getPrecision(int column) {
    try {
      return resultSetMetaData.getPrecision(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public int getScale(int column) {
    try {
      return resultSetMetaData.getScale(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public String getTableName(int column) {
    try {
      return resultSetMetaData.getTableName(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public int getColumnType(int column) {
    try {
      return resultSetMetaData.getColumnType(column);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public String getColumnLabel(int i) {
    try {
      return resultSetMetaData.getColumnLabel(i);
    } catch (SQLException e) {
      throw new MycatException(toMessage(e));
    }
  }

  @Override
  public ResultSetMetaData metaData() {
    return resultSetMetaData;
  }
}
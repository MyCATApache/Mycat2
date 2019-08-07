package io.mycat.datasource.jdbc.resultset;

import java.sql.SQLException;
import java.sql.SQLType;

public interface JdbcResult {

  void updateObject(int columnIndex, Object x,
      SQLType targetSqlType, int scaleOrLength) throws SQLException;


  void updateObject(String columnLabel, Object x,
      SQLType targetSqlType, int scaleOrLength) throws SQLException;

  void updateObject(int columnIndex, Object x, SQLType targetSqlType);

  void updateObject(String columnLabel, Object x,
      SQLType targetSqlType);
}
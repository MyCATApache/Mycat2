package io.mycat.expression;

import java.sql.SQLException;

public interface RowMetaData {

  int getColumnCount() throws SQLException;

  boolean isAutoIncrement(int column) throws SQLException;

  boolean isCaseSensitive(int column) throws SQLException;

  NullableType isNullable(int column) throws SQLException;

  boolean isSigned(int column) throws SQLException;

  int getColumnDisplaySize(int column) throws SQLException;

  String getColumnName(int column) throws SQLException;

  String getSchemaName(int column) throws SQLException;

  int getPrecision(int column) throws SQLException;

  int getScale(int column) throws SQLException;

  String getTableName(int column) throws SQLException;

  int getColumnType(int column) throws SQLException;
}
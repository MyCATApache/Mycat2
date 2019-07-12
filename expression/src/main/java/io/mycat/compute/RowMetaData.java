package io.mycat.compute;

import java.sql.ResultSetMetaData;

public interface RowMetaData {

  int getColumnCount();

  boolean isAutoIncrement(int column);

  boolean isCaseSensitive(int column);

  NullableType isNullable(int column);

  boolean isSigned(int column);

  int getColumnDisplaySize(int column);

  String getColumnName(int column);

  String getSchemaName(int column);

  int getPrecision(int column);

  int getScale(int column);

  String getTableName(int column);

  int getColumnType(int column);

  String getColumnLabel(int i);

  ResultSetMetaData metaData();
}
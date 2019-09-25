package io.mycat.beans.mycat;

import java.sql.ResultSetMetaData;

public interface MycatRowMetaData {

  int getColumnCount();

  boolean isAutoIncrement(int column);

  boolean isCaseSensitive(int column);

  int isNullable(int column);

  boolean isSigned(int column);

  int getColumnDisplaySize(int column);

  String getColumnName(int column);

  String getSchemaName(int column);

  int getPrecision(int column);

  int getScale(int column);

  String getTableName(int column);

  int getColumnType(int column);

  String getColumnLabel(int column);

  ResultSetMetaData metaData();
}
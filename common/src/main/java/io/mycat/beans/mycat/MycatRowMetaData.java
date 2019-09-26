package io.mycat.beans.mycat;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  default String toSimpleText(){
    int columnCount = getColumnCount();
    List list = new ArrayList();
    for (int i = 1; i <= columnCount; i++) {
      Map<String,Object> info = new HashMap<>();


      String schemaName = getSchemaName(i);
      String tableName = getTableName(i);
      String columnName = getColumnName(i);
      int columnType = getColumnType(i);

      info.put("schemaName",schemaName);
      info.put("tableName",tableName);
      info.put("columnName",columnName);
      info.put("columnType",columnType);

      list.add(info);

      String columnLabel = getColumnLabel(i);

      boolean autoIncrement = isAutoIncrement(i);
      boolean caseSensitive = isCaseSensitive(i);
      int nullable = isNullable(i);
      boolean signed = isSigned(i);
      int columnDisplaySize = getColumnDisplaySize(i);
      int precision = getPrecision(i);
      int scale = getScale(i);
    }
    return list.toString();
  }
}
/**
 * Copyright (C) <2020>  <chen junwen>
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

package io.mycat.beans.mycat;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 * date 2020-01-09 23:18
 * column information,like a jdbc
 **/
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
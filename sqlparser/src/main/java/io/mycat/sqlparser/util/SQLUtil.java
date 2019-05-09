package io.mycat.sqlparser.util;

import java.util.Collections;
import java.util.Map;

/**
 * @author jamie12221
 * @date 2019-05-05 11:54
 **/
public class SQLUtil {

  public static CharSequence adjustmentSQL(BufferSQLContext context, boolean removeSchema,
      String oldTableName, String newTableName) {
    Map<String, String> map = Collections.singletonMap(oldTableName, newTableName);
    return adjustmentSQL(context, removeSchema, map);

  }

  public static CharSequence adjustmentSQL(BufferSQLContext context, boolean removeSchema,
      Map<String, String> tableMap) {
    ByteArrayView buffer = context.getBuffer();
    int tableCount = context.getTableCount();
    StringBuilder sb = new StringBuilder();
    HashArray hashArray = context.getHashArray();
    int copyStartIndex = 0;
    int tableStartIndex = 0;
    int schemaStartIndex = 0;
    for (int i = 0; i < tableCount; i++) {
      schemaStartIndex = context.getSchemaNameHashIndex(i);
      if (schemaStartIndex == 0) {
        tableStartIndex = schemaStartIndex = context.getTableNameHashIndex(i);
      } else {
        tableStartIndex = context.getTableNameHashIndex(i);
      }
      int pos = hashArray.getPos(tableStartIndex);
      int size = hashArray.getSize(i);
      sb.append(buffer.getString(copyStartIndex, pos));

      String string = buffer.getString(!removeSchema ? schemaStartIndex : tableStartIndex,
          copyStartIndex = pos + size);
      String table = tableMap.get(string);
      if (table == null) {
        sb.append(string);
      } else {
        sb.append(table);
      }
    }
    return sb;
  }

  public static String removeSchema(BufferSQLContext context) {
    ByteArrayView buffer = context.getBuffer();
    int tableCount = context.getTableCount();
    StringBuilder sb = new StringBuilder();
    HashArray hashArray = context.getHashArray();
    int copyStartIndex = 0;
    int tableStartIndex = 0;
    int schemaStartIndex = 0;
    for (int i = 0; i < tableCount; i++) {
      schemaStartIndex = context.getSchemaNameHashIndex(i);
      if (schemaStartIndex == 0) {
        tableStartIndex = schemaStartIndex = context.getTableNameHashIndex(i);
      } else {
        tableStartIndex = context.getTableNameHashIndex(i);
      }
      int pos = hashArray.getPos(tableStartIndex);
      int size = hashArray.getSize(i);
      sb.append(buffer.getString(copyStartIndex, pos));

      String string = buffer.getString(!true ? schemaStartIndex : tableStartIndex,
          copyStartIndex = pos + size);
      sb.append(string);
    }
    return sb.toString();
  }
}

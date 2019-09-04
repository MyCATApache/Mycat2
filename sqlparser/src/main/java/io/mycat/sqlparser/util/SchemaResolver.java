package io.mycat.sqlparser.util;

public interface SchemaResolver {
    TableRef find(String schema, String table);
  }
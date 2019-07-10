package io.mycat.compute;

import static java.sql.DatabaseMetaData.columnNoNulls;
import static java.sql.DatabaseMetaData.columnNullable;
import static java.sql.DatabaseMetaData.columnNullableUnknown;

import io.mycat.MycatException;

public enum NullableType {
  COLUMN_NO_NULLS, COLUMN_NULLABLE, COLUMN_NULLABLE_UNKNOWN;

  public static NullableType valueOf(int value) {
    switch (value) {
      case columnNoNulls:
        return COLUMN_NO_NULLS;
      case columnNullable:
        return COLUMN_NULLABLE;
      case columnNullableUnknown:
        return COLUMN_NULLABLE_UNKNOWN;
      default:
        throw new MycatException("unknown exception");
    }
  }
}
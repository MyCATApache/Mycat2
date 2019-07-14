package io.mycat.datasource.jdbc;

import java.io.Closeable;

public interface MycatResponse extends Closeable {

  MycatResultSetType getType();
}

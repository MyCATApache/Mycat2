package io.mycat.beans.resultset;

import java.io.Closeable;

public interface MycatResponse extends Closeable {

  MycatResultSetType getType();
}

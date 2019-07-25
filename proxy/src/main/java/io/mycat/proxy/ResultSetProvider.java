package io.mycat.proxy;

import io.mycat.beans.resultset.MycatResultSet;
import java.nio.charset.Charset;

public enum ResultSetProvider {
  INSTANCE;

  public MycatResultSet createDefaultResultSet(int columnCount, int charsetIndex, Charset charset) {
    return new DefResultSet(columnCount, charsetIndex, charset);
  }
}
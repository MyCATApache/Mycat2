package io.mycat.datasource.jdbc;

import io.mycat.datasource.jdbc.JdbcDataSourceManager.SessionProvider;
import java.util.concurrent.atomic.AtomicInteger;

public enum SessionProviderImpl implements SessionProvider {
  INSYANCE;
  final AtomicInteger counter = new AtomicInteger(1);

  @Override
  public int sessionId() {
    return counter.incrementAndGet();
  }
}
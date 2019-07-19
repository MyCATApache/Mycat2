package io.mycat.grid;

import io.mycat.datasource.jdbc.MycatResponse;
import io.mycat.proxy.session.MycatSession;

public interface SQLExecuter {
  public MycatResponse execute() throws Exception;
}
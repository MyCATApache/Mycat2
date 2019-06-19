package io.mycat.command;

import io.mycat.proxy.session.MySQLClientSession;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class MySQLProxyPrepareStatement {

  static class ExecuteArgs{

  }
  static class ReplicaPrepareStatementItem {
    String sql;
    List<MySQLClientSession> mysqls = new ArrayList<>();
    boolean write;
  }
}
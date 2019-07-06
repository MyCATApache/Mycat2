package io.mycat.router.sequence;

import io.mycat.mysqlapi.MySQLAPIRuntime;
import java.util.Map;

public interface SequenceHandler {

  void nextId(String schema, String seqName, SequenceCallback callback);

  void init(MySQLAPIRuntime mySQLAPIRuntime, Map<String, String> properties);
}
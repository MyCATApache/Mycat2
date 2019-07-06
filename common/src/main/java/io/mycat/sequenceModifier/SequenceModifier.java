package io.mycat.sequenceModifier;

import io.mycat.mysqlapi.MySQLAPIRuntime;
import java.util.Map;

public interface SequenceModifier {

  void modify(String schema, String sql, ModifyCallback callback);

  void init(MySQLAPIRuntime mySQLAPIRuntime, Map<String, String> properties);
}
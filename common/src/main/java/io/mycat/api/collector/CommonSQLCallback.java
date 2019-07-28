package io.mycat.api.collector;

import java.util.List;
import java.util.Map;

public interface CommonSQLCallback {

  String getSql();

  void process(List<Map<String, Object>> resultSetList);

  void onError(String errorMessage);

  void onException(Exception e);
}
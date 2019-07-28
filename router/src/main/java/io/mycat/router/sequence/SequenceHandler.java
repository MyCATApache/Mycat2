package io.mycat.router.sequence;

import java.util.Map;

public interface SequenceHandler<Runtime> {

  void nextId(String schema, String seqName, SequenceCallback callback);

  void init(Runtime mySQLAPIRuntime, Map<String, String> properties);
}
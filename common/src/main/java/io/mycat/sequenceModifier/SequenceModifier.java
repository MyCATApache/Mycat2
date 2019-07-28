package io.mycat.sequenceModifier;

import java.util.Map;

public interface SequenceModifier<T> {

  void modify(String schema, String sql, ModifyCallback callback);

  void init(T runtime, Map<String, String> properties);
}
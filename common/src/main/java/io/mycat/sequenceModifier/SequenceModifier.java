package io.mycat.sequenceModifier;

import java.util.Map;

public interface SequenceModifier {

  void modify(String sql, ModifyCallback callback);

  void init(Map<String, String> properties);
}
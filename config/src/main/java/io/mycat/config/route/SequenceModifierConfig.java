package io.mycat.config.route;

import java.util.Map;

public class SequenceModifierConfig {

  String sequenceModifierClazz;
  Map<String, String> SequenceModifierProperties;

  public String getSequenceModifierClazz() {
    return sequenceModifierClazz;
  }

  public void setSequenceModifierClazz(String sequenceModifierClazz) {
    this.sequenceModifierClazz = sequenceModifierClazz;
  }

  public Map<String, String> getSequenceModifierProperties() {
    return SequenceModifierProperties;
  }

  public void setSequenceModifierProperties(Map<String, String> sequenceModifierProperties) {
    this.SequenceModifierProperties = sequenceModifierProperties;
  }
}
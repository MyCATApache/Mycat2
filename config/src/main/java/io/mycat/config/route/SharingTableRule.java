package io.mycat.config.route;

import java.util.List;
import java.util.Map;

/**
 * @author jamie12221 date 2019-05-03 14:19
 **/
public class SharingTableRule {

  String tableName;
  String function;
  String sequenceClass;
  Map<String, String> sequenceProperties;

  public String getFunction() {
    return function;
  }

  public void setFunction(String function) {
    this.function = function;
  }

  List<ShardingRule> rules;

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }


  public List<ShardingRule> getRules() {
    return rules;
  }

  public void setRules(List<ShardingRule> rules) {
    this.rules = rules;
  }


  public String getSequenceClass() {
    return sequenceClass;
  }

  public void setSequenceClass(String sequenceClass) {
    this.sequenceClass = sequenceClass;
  }


  public Map<String, String> getSequenceProperties() {
    return sequenceProperties;
  }

  public void setSequenceProperties(Map<String, String> sequenceProperties) {
    this.sequenceProperties = sequenceProperties;
  }
}

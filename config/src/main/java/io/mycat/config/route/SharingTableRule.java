package io.mycat.config.route;

import java.util.List;

/**
 * @author jamie12221
 *  date 2019-05-03 14:19
 **/
public class SharingTableRule {

  String tableName;
  String function;

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
}

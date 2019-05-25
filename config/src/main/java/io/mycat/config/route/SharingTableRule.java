package io.mycat.config.route;

import java.util.List;

/**
 * @author jamie12221
 *  date 2019-05-03 14:19
 **/
public class SharingTableRule {
  String name;
  String funtion;

  public String getFuntion() {
    return funtion;
  }

  public void setFuntion(String funtion) {
    this.funtion = funtion;
  }

  List<ShardingRule> rules;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }


  public List<ShardingRule> getRules() {
    return rules;
  }

  public void setRules(List<ShardingRule> rules) {
    this.rules = rules;
  }
}

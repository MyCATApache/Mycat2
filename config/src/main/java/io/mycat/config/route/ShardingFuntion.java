package io.mycat.config.route;

import java.util.Map;

/**
 * @author jamie12221
 *  date 2019-05-03 14:58
 **/
public class ShardingFuntion implements Cloneable{
   String name;

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getClazz() {
    return clazz;
  }

  public void setClazz(String clazz) {
    this.clazz = clazz;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  String clazz;
  Map<String,String>properties;
  Map<String, String> ranges;
  SubShardingFuntion subFuntion;

  public SubShardingFuntion getSubFuntion() {
    return subFuntion;
  }

  public void setSubFuntion(SubShardingFuntion subFuntion) {
    this.subFuntion = subFuntion;
  }

  public Map<String, String> getRanges() {
    return ranges;
  }

  public void setRanges(Map<String, String> ranges) {
    this.ranges = ranges;
  }
}

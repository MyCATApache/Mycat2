package io.mycat.config.route;

import java.util.Map;

/**
 * @author jamie12221 date 2019-05-03 14:58
 **/
public class SubShardingFuntion implements Cloneable {

  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();
  }

  public String getClazz() {
    return clazz;
  }

  public void setClazz(String clazz) {
    this.clazz = clazz;
  }


  String clazz;
  Map<String, String> properties;
  Map<String, String> ranges;

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  /**
   * Getter for property 'ranges'.
   *
   * @return Value for property 'ranges'.
   */
  public Map<String, String> getRanges() {
    return ranges;
  }

  /**
   * Setter for property 'ranges'.
   *
   * @param ranges Value to set for property 'ranges'.
   */
  public void setRanges(Map<String, String> ranges) {
    this.ranges = ranges;
  }

  SubShardingFuntion subFuntion;

  public SubShardingFuntion getSubFuntion() {
    return subFuntion;
  }

  public void setSubFuntion(SubShardingFuntion subFuntion) {
    this.subFuntion = subFuntion;
  }

  @Override
  public String toString() {
    return "SubShardingFuntion{" +
        "clazz='" + clazz + '\'' +
        ", properties=" + properties +
        ", subFuntion=" + subFuntion +
        '}';
  }
}

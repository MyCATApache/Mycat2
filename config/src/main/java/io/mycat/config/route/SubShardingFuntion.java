package io.mycat.config.route;

import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 *  date 2019-05-03 14:58
 **/
public class SubShardingFuntion implements Cloneable{
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
  List<Map<String,String>> properties;

  public List<Map<String, String>> getProperties() {
    return properties;
  }

  public void setProperties(List<Map<String, String>> properties) {
    this.properties = properties;
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

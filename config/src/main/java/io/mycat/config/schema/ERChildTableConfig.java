package io.mycat.config.schema;

/**
 * @author jamie12221
 *  date 2019-05-02 23:06
 **/
public class ERChildTableConfig {

  String name;
  String joinKey;
  String parentKey;
  String primaryKey;
  boolean defaultlimit;

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getJoinKey() {
    return joinKey;
  }

  public void setJoinKey(String joinKey) {
    this.joinKey = joinKey;
  }

  public String getParentKey() {
    return parentKey;
  }

  public void setParentKey(String parentKey) {
    this.parentKey = parentKey;
  }

  public String getPrimaryKey() {
    return primaryKey;
  }

  public void setPrimaryKey(String primaryKey) {
    this.primaryKey = primaryKey;
  }

  public boolean isDefaultlimit() {
    return defaultlimit;
  }

  public void setDefaultlimit(boolean defaultlimit) {
    this.defaultlimit = defaultlimit;
  }

  @Override
  public String toString() {
    return "ERChildTableConfig{" +
               "name='" + name + '\'' +
               ", joinKey='" + joinKey + '\'' +
               ", parentKey='" + parentKey + '\'' +
               ", primaryKey='" + primaryKey + '\'' +
               ", defaultlimit=" + defaultlimit +
               '}';
  }
}

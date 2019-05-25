package io.mycat.config.route;

import java.util.Objects;

/**
 * @author jamie12221
 *  date 2019-05-03 22:58
 **/
public class ShardingAnnotation {
  String name;
  AnnotationType type;

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ShardingAnnotation that = (ShardingAnnotation) o;
    return Objects.equals(name, that.name) &&
               type == that.type;
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, type);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public AnnotationType getType() {
    return type;
  }

  public void setType(AnnotationType type) {
    this.type = type;
  }
}

package io.mycat.config.route;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jamie12221
 * @date 2019-05-02 14:35
 **/
public class DynamicAnnotationConfig implements Cloneable {

  String name;
  String pattern;
  AnnotationType type = AnnotationType.OTHER;
  List<String> groupNameList = new ArrayList<>();
  final static Pattern extGroupName = Pattern.compile("<[\\u4e00-\\u9fa5_a-zA-Z0-9]+>");

  public AnnotationType getType() {
    return type;
  }

  public void setType(AnnotationType type) {
    this.type = type;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getPattern() {
    return pattern;
  }

  public void setPattern(String pattern) {
    this.pattern = pattern;
    Matcher matcher = extGroupName.matcher(pattern);
    while (matcher.find()) {
      String group = matcher.group();
      String key = group.substring(1, group.length() - 1);
      groupNameList.add(key);
    }
  }

  public List<String> getGroupNameList() {
    return groupNameList;
  }

  @Override
  public Object clone() {
    try {
      return super.clone();
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }
}

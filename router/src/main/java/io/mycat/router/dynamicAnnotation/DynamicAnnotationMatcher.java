package io.mycat.router.dynamicAnnotation;

import io.mycat.config.route.DynamicAnnotationConfig;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author jamie12221
 * @date 2019-05-02 14:50
 **/
public class DynamicAnnotationMatcher {

  List<DynamicAnnotationConfig> config;
  Pattern pattern;
  List<String> groupNameList = new ArrayList<>();

  static final ThreadLocal<Map<String, String>> threadLocal = new ThreadLocal();

  public DynamicAnnotationMatcher(List<DynamicAnnotationConfig> config) {
    this.config = config;
    List<String> patterns = new ArrayList<>();
    Pattern extGroupName = Pattern.compile("<[\\u4e00-\\u9fa5_a-zA-Z0-9]+>");
    for (DynamicAnnotationConfig dynamicAnnotation : config) {
      String name = dynamicAnnotation.getName();
      String pattern = dynamicAnnotation.getPattern();
      Matcher matcher = extGroupName.matcher(pattern);
      while (matcher.find()) {
        String group = matcher.group();
        groupNameList.add(group.substring(1, group.length() - 1));
      }
      String s = pattern;
      patterns.add(s);
    }
    String regex = "(" + patterns.stream().collect(Collectors.joining("|")) + ")*";
    pattern = Pattern.compile(regex);

  }

  public Map<String, String> match(CharSequence input) {
    Map<String, String> stringStringMap = threadLocal.get();
    if (stringStringMap == null) {
      stringStringMap = new HashMap<>();
      threadLocal.set(stringStringMap);
    } else {
      stringStringMap.clear();
    }
    Matcher matcher = pattern.matcher(input);
    int size = groupNameList.size();
    while (matcher.find()) {
      for (int i = 0; i < size; i++) {
        String group = groupNameList.get(i);
        String value = matcher.group(group);
        if (value != null) {
          stringStringMap.put(group, value);
        }
      }
    }
    return stringStringMap;
  }

  public static void main(String[] args) {

  }

}

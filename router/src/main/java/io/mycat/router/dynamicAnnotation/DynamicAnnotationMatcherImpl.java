/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router.dynamicAnnotation;

import io.mycat.config.route.AnnotationType;
import io.mycat.config.route.DynamicAnnotationConfig;
import io.mycat.router.DynamicAnnotationMatcher;
import io.mycat.router.DynamicAnnotationResult;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author jamie12221
 * @date 2019-05-02 14:50
 **/
public class DynamicAnnotationMatcherImpl implements DynamicAnnotationMatcher {

  final Pattern pattern;
  final List<String> groupNameList = new ArrayList<>();
  final Map<String, AnnotationType> groupNameTypeMap = new HashMap<>();
  public static final DynamicAnnotationMatcherImpl EMPTY = new DefaultDynamicAnnotationMatcher(
      Collections.EMPTY_LIST);

  public DynamicAnnotationResult match(String input) {
    DynamicAnnotationResultImpl result = threadLocal.get();
    if (result == null) {
      result = new DynamicAnnotationResultImpl();
      threadLocal.set(result);
    } else {
      result.clear();
    }
    Matcher matcher = pattern.matcher(input);
    int size = groupNameList.size();
    while (matcher.find()) {
      for (int i = 0; i < size; i++) {
        String group = groupNameList.get(i);
        String value = matcher.group(group);
        if (value != null) {
          result.put(group, value, groupNameTypeMap.get(group));
          result.setSql(input);
        }
      }
    }
    return result;
  }

  static final ThreadLocal<DynamicAnnotationResultImpl> threadLocal = new ThreadLocal();

  public DynamicAnnotationMatcherImpl(List<DynamicAnnotationConfig> config) {
    List<String> patterns = new ArrayList<>();

    for (DynamicAnnotationConfig dynamicAnnotation : config) {
      String name = dynamicAnnotation.getName();
      String pattern = dynamicAnnotation.getPattern();
      List<String> groupNameList = dynamicAnnotation.getGroupNameList();
      this.groupNameList.addAll(groupNameList);
      for (String key : groupNameList) {
        groupNameTypeMap.put(key, dynamicAnnotation.getType());
      }
      patterns.add(pattern);
    }
    String regex = "";
    if (!config.isEmpty()) {
      if (config.size() == 1) {
        regex = patterns.get(0);
      } else {
        regex = "(" + String.join("|", patterns) + ")*";
      }
    }
    pattern = Pattern.compile(regex);
  }

  private static class DefaultDynamicAnnotationMatcher extends DynamicAnnotationMatcherImpl {

    public DefaultDynamicAnnotationMatcher(List<DynamicAnnotationConfig> config) {
      super(config);
    }

    @Override
    public DynamicAnnotationResult match(String input) {
      return DynamicAnnotationResultImpl.EMPTY;
    }
  }

  public static void main(String[] args) {

  }


}

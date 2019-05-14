package io.mycat.router;

import io.mycat.config.route.AnnotationType;

/**
 * @author jamie12221
 * @date 2019-05-05 13:33 动态注解处理结果
 **/
public interface DynamicAnnotationResult {

  void clear();

  String get(String key);

  void put(String key, String value, AnnotationType type);
}
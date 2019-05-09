package io.mycat.router;

import io.mycat.config.route.AnnotationType;

public interface DynamicAnnotationResult {

  public void clear();

  public String get(String key);

  public void put(String key, String value, AnnotationType type);
}
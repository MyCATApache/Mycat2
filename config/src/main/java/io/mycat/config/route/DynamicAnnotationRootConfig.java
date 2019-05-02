package io.mycat.config.route;

import io.mycat.config.Configurable;
import java.util.List;

/**
 * @author jamie12221
 * @date 2019-05-02 14:34
 **/
public class DynamicAnnotationRootConfig implements Configurable {
  private List<DynamicAnnotationConfig> dynamicAnnotations;

  public List<DynamicAnnotationConfig> getDynamicAnnotations() {
    return dynamicAnnotations;
  }

  public void setDynamicAnnotations(
      List<DynamicAnnotationConfig> dynamicAnnotations) {
    this.dynamicAnnotations = dynamicAnnotations;
  }
}

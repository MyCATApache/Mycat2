package io.mycat.config.proxy;

import io.mycat.config.ConfigurableRoot;
import java.util.Map;

/**
 * @author jamie12221
 * date 2019-05-26 23:58
 **/
public class MysqlServerVariablesRootConfig implements ConfigurableRoot {

  Map<String, String> variables;

  public Map<String, String> getVariables() {
    return variables;
  }

  public void setVariables(Map<String, String> variables) {
    this.variables = variables;
  }
}

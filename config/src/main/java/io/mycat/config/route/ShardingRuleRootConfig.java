package io.mycat.config.route;

import io.mycat.config.Configurable;
import java.util.List;

/**
 * @author jamie12221
 * @date 2019-05-03 14:18
 **/
public class ShardingRuleRootConfig implements Configurable {

  String sqlInterceptorClass;
   List<SharingTableRule> tableRules;

  public List<SharingTableRule> getTableRules() {
    return tableRules;
  }

  public void setTableRules(List<SharingTableRule> tableRules) {
    this.tableRules = tableRules;
  }

  public String getSqlInterceptorClass() {
    return sqlInterceptorClass;
  }

  public void setSqlInterceptorClass(String sqlInterceptorClass) {
    this.sqlInterceptorClass = sqlInterceptorClass;
  }
}

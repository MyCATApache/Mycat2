package io.mycat.config.route;

import io.mycat.config.ConfigurableRoot;
import java.util.List;

/**
 * @author jamie12221
 *  date 2019-05-03 14:54
 **/
public class SharingFuntionRootConfig extends ConfigurableRoot {

  List<ShardingFuntion> functions;

  public List<ShardingFuntion> getFunctions() {
    return functions;
  }

  public void setFunctions(List<ShardingFuntion> functions) {
    this.functions = functions;
  }
}

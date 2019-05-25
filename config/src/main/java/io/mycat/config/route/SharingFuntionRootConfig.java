package io.mycat.config.route;

import io.mycat.config.Configurable;
import java.util.List;

/**
 * @author jamie12221
 *  date 2019-05-03 14:54
 **/
public class SharingFuntionRootConfig implements Configurable {
  List<ShardingFuntion> funtions;

  public List<ShardingFuntion> getFuntions() {
    return funtions;
  }

  public void setFuntions(List<ShardingFuntion> funtions) {
    this.funtions = funtions;
  }
}

package io.mycat.config.schema;

import io.mycat.config.Configurable;
import java.util.ArrayList;
import java.util.List;

public class DataNodeRootConfig implements Configurable {

  private List<DataNodeConfig> dataNodes = new ArrayList<DataNodeConfig>();

  /**
   * Getter for property 'dataNodes'.
   *
   * @return Value for property 'dataNodes'.
   */
  public List<DataNodeConfig> getDataNodes() {
    return dataNodes;
  }

  /**
   * Setter for property 'dataNodes'.
   *
   * @param dataNodes Value to set for property 'dataNodes'.
   */
  public void setDataNodes(List<DataNodeConfig> dataNodes) {
    this.dataNodes = dataNodes;
  }
}
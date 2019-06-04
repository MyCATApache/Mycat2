package io.mycat.router.staticAnnotation;

public class MycatProxyStaticAnnotation {

  String schema;
  String shardingKey;
  String shardingRangeKeyStart;
  String shardingRangeKeyEnd;
  Boolean runOnMaster;
  String dataNode;
  String balance;
  String routeSQL;

  public void clear() {
    this.schema = null;
    this.shardingKey = null;
    this.shardingRangeKeyStart = null;
    this.shardingRangeKeyEnd = null;
    this.runOnMaster = false;
    this.dataNode = null;
    this.balance = null;
    this.routeSQL = null;
  }

  /**
   * Getter for property 'schema'.
   *
   * @return Value for property 'schema'.
   */
  public String getSchema() {
    return schema;
  }

  /**
   * Setter for property 'schema'.
   *
   * @param schema Value to set for property 'schema'.
   */
  public void setSchema(String schema) {
    this.schema = schema;
  }

  /**
   * Getter for property 'shardingKey'.
   *
   * @return Value for property 'shardingKey'.
   */
  public String getShardingKey() {
    return shardingKey;
  }

  /**
   * Setter for property 'shardingKey'.
   *
   * @param shardingKey Value to set for property 'shardingKey'.
   */
  public void setShardingKey(String shardingKey) {
    this.shardingKey = shardingKey;
  }

  /**
   * Getter for property 'shardingRangeKeyStart'.
   *
   * @return Value for property 'shardingRangeKeyStart'.
   */
  public String getShardingRangeKeyStart() {
    return shardingRangeKeyStart;
  }

  /**
   * Setter for property 'shardingRangeKeyStart'.
   *
   * @param shardingRangeKeyStart Value to set for property 'shardingRangeKeyStart'.
   */
  public void setShardingRangeKeyStart(String shardingRangeKeyStart) {
    this.shardingRangeKeyStart = shardingRangeKeyStart;
  }

  /**
   * Getter for property 'shardingRangeKeyEnd'.
   *
   * @return Value for property 'shardingRangeKeyEnd'.
   */
  public String getShardingRangeKeyEnd() {
    return shardingRangeKeyEnd;
  }

  /**
   * Setter for property 'shardingRangeKeyEnd'.
   *
   * @param shardingRangeKeyEnd Value to set for property 'shardingRangeKeyEnd'.
   */
  public void setShardingRangeKeyEnd(String shardingRangeKeyEnd) {
    this.shardingRangeKeyEnd = shardingRangeKeyEnd;
  }

  /**
   * Getter for property 'runOnMaster'.
   * no setting ->null
   * @return Value for property 'runOnMaster'.
   */
  public Boolean isRunOnMaster() {
    return runOnMaster;
  }

  /**
   * Setter for property 'runOnMaster'
   *
   * no setting ->null
   *
   * @param runOnMaster Value to set for property 'runOnMaster'.
   */
  public void setRunOnMaster(Boolean runOnMaster) {
    this.runOnMaster = runOnMaster;
  }

  /**
   * Getter for property 'dataNode'.
   *
   * @return Value for property 'dataNode'.
   */
  public String getDataNode() {
    return dataNode;
  }

  /**
   * Setter for property 'dataNode'.
   *
   * @param dataNode Value to set for property 'dataNode'.
   */
  public void setDataNode(String dataNode) {
    this.dataNode = dataNode;
  }

  /**
   * Getter for property 'balance'.
   *
   * @return Value for property 'balance'.
   */
  public String getBalance() {
    return balance;
  }

  /**
   * Setter for property 'balance'.
   *
   * @param balance Value to set for property 'balance'.
   */
  public void setBalance(String balance) {
    this.balance = balance;
  }

  /**
   * Getter for property 'routeSQL'.
   *
   * @return Value for property 'routeSQL'.
   */
  public String getRouteSQL() {
    return routeSQL;
  }

  /**
   * Setter for property 'routeSQL'.
   *
   * @param routeSQL Value to set for property 'routeSQL'.
   */
  public void setRouteSQL(String routeSQL) {
    this.routeSQL = routeSQL;
  }
}
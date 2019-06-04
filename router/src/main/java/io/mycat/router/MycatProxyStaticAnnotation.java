package io.mycat.router;

import io.mycat.sqlparser.util.SQLMapAnnotation;

public class MycatProxyStaticAnnotation implements SQLMapAnnotation.PutKeyValueAble {

//  final Map<String, Object> map = new HashMap<>();
//  final static String SCHEMA = "schema";
//  final static String SHARDING_KEY = "shardingKey";
//  final static String SHARDING_RANGE_KEY_START = "shardingRangeKeyStart";
//  final static String SHARDING_RANGE_KEY_END = "shardingRangeKeyEnd";
//  final static String RUN_ON_MASTER = "runOnMaster";
//  final static String DATA_NODE = "dataNode";
//  final static String BALANCE = "balance";
//  final static String ROUTE_SQL = "routeSQL";

  String schema;
  String shardingKey;
  String shardingRangeKeyStart;
  String shardingRangeKeyEnd;
  Boolean runOnMaster;
  String balance;
  String routeSQL;


  public void clear() {
    schema = null;
    shardingKey = null;
    shardingRangeKeyStart = null;
    shardingRangeKeyEnd = null;
    runOnMaster = null;
    balance = null;
    routeSQL = null;
  }

  @Override
  public void put(String key, long number) {
    String value = Long.toString(number);
    switch (key) {
      case "schema": {
        schema = value;
        break;
      }
      case "shardingKey": {
        shardingKey = value;
        break;
      }
      case "shardingRangeKeyStart": {
        shardingRangeKeyStart = value;
        break;
      }
      case "shardingRangeKeyEnd": {
        shardingRangeKeyEnd = value;
        break;
      }
      case "runOnMaster": {
        runOnMaster = "1".equals(value);
        break;
      }
      case "balance": {
        balance = value;
        break;
      }
      case "routeSQL": {
        routeSQL = value;
        break;
      }
    }
  }

  @Override
  public void put(String key, String value) {
    switch (key) {
      case "schema": {
        schema = value;
        break;
      }
      case "shardingKey": {
        shardingKey = value;
        break;
      }
      case "shardingRangeKeyStart": {
        shardingRangeKeyStart = value;
        break;
      }
      case "shardingRangeKeyEnd": {
        shardingRangeKeyEnd = value;
        break;
      }
      case "runOnMaster": {
        runOnMaster = "1".equals(value);
        break;
      }
      case "balance": {
        balance = value;
        break;
      }
      case "routeSQL": {
        routeSQL = value;
        break;
      }
    }
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
   * Getter for property 'shardingKey'.
   *
   * @return Value for property 'shardingKey'.
   */
  public String getShardingKey() {
    return shardingKey;
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
   * Getter for property 'shardingRangeKeyEnd'.
   *
   * @return Value for property 'shardingRangeKeyEnd'.
   */
  public String getShardingRangeKeyEnd() {
    return shardingRangeKeyEnd;
  }

  /**
   * Getter for property 'runOnMaster'.
   *
   * @return Value for property 'runOnMaster'.
   */
  public Boolean getRunOnMaster() {
    return runOnMaster;
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
   * Getter for property 'routeSQL'.
   *
   * @return Value for property 'routeSQL'.
   */
  public String getRouteSQL() {
    return routeSQL;
  }
}
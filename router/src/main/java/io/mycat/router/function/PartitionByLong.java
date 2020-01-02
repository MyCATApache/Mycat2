package io.mycat.router.function;

import io.mycat.router.RuleFunction;
import io.mycat.router.util.PartitionUtil;

import java.util.Map;

/**
 * @author jamie12221 date 2019-05-02 23:36
 **/
public class PartitionByLong extends RuleFunction {

  private PartitionUtil partitionUtil;
  @Override
  public String name() {
    return "PartitionByLong";
  }

  @Override
  public void init(Map<String, String> properties, Map<String, String> ranges) {
    int[] count = (toIntArray(properties.get("partitionCount")));
    int[] length = toIntArray(properties.get("partitionLength"));
    partitionUtil = new PartitionUtil(count, length);
  }

  @Override
  public int calculate(String columnValue) {
    try {
      long key = Long.parseLong(columnValue);
      key = (key >>> 32) ^ key;
      return partitionUtil.partition(key);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "columnValue:" + columnValue + " Please eliminate any quote and non number within it.",
          e);
    }
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return RuleFunction.calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return partitionUtil.getPartitionNum();
  }
}

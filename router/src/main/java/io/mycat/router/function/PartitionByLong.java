package io.mycat.router.function;

import io.mycat.router.RuleAlgorithm;
import io.mycat.router.util.PartitionUtil;
import java.util.Map;

/**
 * @author jamie12221
 *  date 2019-05-02 23:36
 **/
public class PartitionByLong extends RuleAlgorithm {

  protected int[] count;
  protected int[] length;
  protected PartitionUtil partitionUtil;

  private static int[] toIntArray(String string) {
    String[] strs = io.mycat.util.SplitUtil.split(string, ',', true);
    int[] ints = new int[strs.length];
    for (int i = 0; i < strs.length; ++i) {
      ints[i] = Integer.parseInt(strs[i]);
    }
    return ints;
  }

  public void setPartitionCount(String partitionCount) {
    this.count = toIntArray(partitionCount);
  }

  public void setPartitionLength(String partitionLength) {
    this.length = toIntArray(partitionLength);
  }

  @Override
  public String name() {
    return this.getClass().getSimpleName();
  }

  @Override
  public void init(Map<String, String> properties) {
    setPartitionCount(properties.get("partitionCount"));
    setPartitionLength(properties.get("partitionLength"));
    partitionUtil = new PartitionUtil(count, length);
  }


  @Override
  public int calculate(String columnValue) {
    try {
      long key = Long.parseLong(columnValue);
      return partitionUtil.partition(key);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "columnValue:" + columnValue + " Please eliminate any quote and non number within it.",
          e);
    }
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return RuleAlgorithm.calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return this.count.length;
  }
}

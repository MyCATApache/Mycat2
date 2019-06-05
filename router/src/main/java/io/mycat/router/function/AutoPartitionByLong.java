package io.mycat.router.function;

import io.mycat.router.RuleAlgorithm;
import java.util.Map;

public class AutoPartitionByLong extends RuleAlgorithm {

  private LongRange[] longRanges;
  private int defaultNode = -1;
  private int partitionCount;

  @Override
  public String name() {
    return "AutoPartitionByLong";
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.defaultNode = Integer.parseInt(prot.get("defaultNode"));
    this.longRanges = LongRange.getLongRanges(ranges);
    this.partitionCount = LongRange.getPartitionCount(this.longRanges);
  }


  @Override
  public int calculate(String columnValue) {
    try {
      long value = Long.parseLong(columnValue);
      for (LongRange longRang : this.longRanges) {
        if (value <= longRang.valueEnd && value >= longRang.valueStart) {
          return longRang.nodeIndx;
        }
      }
      return defaultNode;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          "columnValue:" + columnValue + " Please eliminate any quote and non number within it.",
          e);
    }
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return partitionCount;
  }

}
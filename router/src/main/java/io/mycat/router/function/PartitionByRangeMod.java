package io.mycat.router.function;

import io.mycat.router.RuleFunction;

import java.math.BigInteger;
import java.util.Map;

public class PartitionByRangeMod extends RuleFunction {

  private GroupSizeRange[] longRanges;
  private int defaultNode = -1;
  private int partitionCount;

  @Override
  public String name() {
    return "PartitionByRangeMod";
  }

  @Override
  public int calculate(String columnValue) {
    long value = Long.parseLong(columnValue);
    int nodeIndex = 0;
    for (GroupSizeRange longRang : this.longRanges) {
      if (value <= longRang.valueEnd && value >= longRang.valueStart) {
        BigInteger bigNum = new BigInteger(columnValue).abs();
        int innerIndex = (bigNum.mod(BigInteger.valueOf(longRang.groupSize))).intValue();
        return nodeIndex + innerIndex;
      } else {
        nodeIndex += longRang.groupSize;
      }
    }
    // 数据超过范围，暂时使用配置的默认节点
    if (defaultNode >= 0) {
      return defaultNode;
    }
    return -1;
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return partitionCount;
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.defaultNode = Integer.parseInt(prot.get("defaultNode"));
    this.longRanges = GroupSizeRange.getGroupSizeRange(ranges);
    this.partitionCount = GroupSizeRange.getPartitionCount(this.longRanges);
  }
}
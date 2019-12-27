package io.mycat.router.function;

import io.mycat.router.RuleFunction;
import java.util.Map;

public class PartitionByPrefixPattern extends RuleFunction {

  private static final int PARTITION_LENGTH = 1024;
  private int patternValue = PARTITION_LENGTH;// 分区长度，取模数值(默认为1024)
  private int prefixLength;// 字符前几位进行ASCII码取和
  private NodeIndexRange[] longRongs;
  private int nPartition;

  @Override
  public String name() {
    return "PartitionByPrefixPattern";
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.patternValue = Integer.parseInt(prot.get("patternValue"));
    this.prefixLength = Integer.parseInt(prot.get("prefixLength"));
    this.longRongs = NodeIndexRange.getLongRanges(ranges);
    this.nPartition = NodeIndexRange.getPartitionCount(this.longRongs);
  }

  @Override
  public int calculate(String columnValue) {
    int length = Math.min(columnValue.length(), prefixLength);
    int sum = 0;
    for (int i = 0; i < length; i++) {
      sum = sum + columnValue.charAt(i);
    }
    for (NodeIndexRange longRang : this.longRongs) {
      long hash = sum % patternValue;
      if (hash <= longRang.valueEnd && hash >= longRang.valueStart) {
        return longRang.nodeIndex;
      }
    }
    return -1;
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return nPartition;
  }
}
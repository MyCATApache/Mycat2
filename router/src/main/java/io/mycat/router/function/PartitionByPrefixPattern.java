package io.mycat.router.function;

import io.mycat.router.RuleAlgorithm;
import java.util.Map;

public class PartitionByPrefixPattern extends RuleAlgorithm {

  private static final int PARTITION_LENGTH = 1024;
  private int patternValue = PARTITION_LENGTH;// 分区长度，取模数值(默认为1024)
  private int prefixLength;// 字符前几位进行ASCII码取和
  private LongRange[] longRongs;
  private int nPartition;

  @Override
  public String name() {
    return "PartitionByPrefixPattern";
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.patternValue = Integer.parseInt(prot.get("patternValue"));
    this.prefixLength = Integer.parseInt(prot.get("prefixLength"));
    this.longRongs = LongRange.getLongRanges(ranges);
    this.nPartition = LongRange.getPartitionCount(this.longRongs);
  }

  @Override
  public int calculate(String columnValue) {
    int length = Math.min(columnValue.length(), prefixLength);
    int sum = 0;
    for (int i = 0; i < length; i++) {
      sum = sum + columnValue.charAt(i);
    }
    for (LongRange longRang : this.longRongs) {
      long hash = sum % patternValue;
      if (hash <= longRang.valueEnd && hash >= longRang.valueStart) {
        return longRang.nodeIndx;
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
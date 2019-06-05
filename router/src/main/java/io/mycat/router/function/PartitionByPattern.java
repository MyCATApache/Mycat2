package io.mycat.router.function;

import io.mycat.router.RuleAlgorithm;
import java.util.Map;
import java.util.regex.Pattern;

public class PartitionByPattern extends RuleAlgorithm {

  private static final int PARTITION_LENGTH = 1024;
  private static final Pattern PATTERN = Pattern.compile("[0-9]*");
  private int patternValue = PARTITION_LENGTH;// 分区长度，取模数值
  private NodeIndexRange[] longRanges;
  private int nPartition;
  private int defaultNode = 0;// 包含非数值字符，默认存储节点

  private static boolean isNumeric(String str) {
    return PATTERN.matcher(str).matches();
  }

  @Override
  public String name() {
    return "PartitionByPattern";
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.patternValue = Integer.parseInt(prot.get("patternValue"));
    this.defaultNode = Integer.parseInt(prot.get("defaultNode"));
    this.longRanges = NodeIndexRange.getLongRanges(ranges);
    this.nPartition = NodeIndexRange.getPartitionCount(this.longRanges);
  }

  @Override
  public int calculate(String columnValue) {
    if (!isNumeric(columnValue)) {
      return defaultNode;
    }
    long value = Long.parseLong(columnValue);
    for (NodeIndexRange longRang : this.longRanges) {
      long hash = value % patternValue;
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
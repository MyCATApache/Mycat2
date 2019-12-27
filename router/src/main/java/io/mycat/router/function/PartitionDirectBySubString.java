package io.mycat.router.function;

import io.mycat.router.RuleFunction;
import java.util.Map;

public class PartitionDirectBySubString extends RuleFunction {

  // 字符子串起始索引（zero-based)
  private int startIndex;
  // 字串长度
  private int size;
  // 分区数量
  private int partitionCount;
  // 默认分区（在分区数量定义时，字串标示的分区编号不在分区数量内时，使用默认分区）
  private int defaultNode;

  @Override
  public String name() {
    return "PartitionDirectBySubString";
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.startIndex = Integer.parseInt(prot.get("startIndex"));
    this.size = Integer.parseInt(prot.get("size"));
    this.partitionCount = Integer.parseInt(prot.get("partitionCount"));
    this.defaultNode = Integer.parseInt(prot.get("defaultNode"));
  }

  @Override
  public int calculate(String columnValue) {
    String partitionSubString = columnValue.substring(startIndex, startIndex + size);
    int partition = Integer.parseInt(partitionSubString, 10);
    return partitionCount > 0 && partition >= partitionCount
        ? defaultNode : partition;
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
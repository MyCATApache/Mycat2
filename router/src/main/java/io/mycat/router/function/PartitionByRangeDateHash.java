package io.mycat.router.function;

import com.google.common.hash.Hashing;
import io.mycat.router.RuleAlgorithm;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Map;

public class PartitionByRangeDateHash extends RuleAlgorithm {

  private DateTimeFormatter formatter;
  private long beginDate;
  private int groupPartionSize;
  private int partionDay;

  @Override
  public String name() {
    return "PartitionByRangeDateHash";
  }

  @Override
  public int calculate(String columnValue) {
    long targetTime = formatter.parse(columnValue).get(ChronoField.DAY_OF_YEAR);
    int targetPartition = (int) ((targetTime - beginDate) / partionDay);
    int innerIndex = Hashing.consistentHash(targetTime, groupPartionSize);
    return targetPartition * groupPartionSize + innerIndex;
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    int begin = calculateStart(beginValue);
    int end = calculateEnd(endValue);
    if (begin == -1 || end == -1) {
      return new int[0];
    }

    if (end >= begin) {
      int len = end - begin + 1;
      int[] re = new int[len];

      for (int i = 0; i < len; i++) {
        re[i] = begin + i;
      }

      return re;
    } else {
      return new int[0];
    }
  }

  @Override
  public int getPartitionNum() {
    return -1;
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.formatter = DateTimeFormatter.ofPattern(prot.get("dateFormat"));
    this.beginDate = this.formatter.parse(prot.get("beginDate")).get(ChronoField.DAY_OF_YEAR);
    this.groupPartionSize = Integer.parseInt(prot.get("groupPartionSize"));
    this.partionDay = Integer.parseInt(prot.get("partionDay"));
    if (this.groupPartionSize <= 0) {
      throw new RuntimeException("groupPartionSize must >0,but cur is " + this.groupPartionSize);
    }
  }

  public int calculateStart(String columnValue) {
    long targetTime = formatter.parse(columnValue).get(ChronoField.DAY_OF_YEAR);
    return innerCaculateStart(targetTime);
  }

  private int innerCaculateStart(long targetTime) {
    int targetPartition = (int) ((targetTime - beginDate) / partionDay);
    return targetPartition * groupPartionSize;
  }

  public int calculateEnd(String columnValue) {
    long targetTime = formatter.parse(columnValue).get(ChronoField.DAY_OF_YEAR);
    return innerCaculateEnd(targetTime);
  }

  private int innerCaculateEnd(long targetTime) {
    int targetPartition = (int) ((targetTime - beginDate) / partionDay);
    return (targetPartition + 1) * groupPartionSize - 1;
  }
}
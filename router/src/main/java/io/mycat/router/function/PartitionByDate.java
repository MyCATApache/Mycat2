package io.mycat.router.function;

import io.mycat.router.RuleFunction;
import io.mycat.router.util.StringUtil;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class PartitionByDate extends RuleFunction {

  private static final long ONE_DAY = 86400000;
  private long beginDate;
  private long partionTime;
  private long endDate;
  private int nCount;
  private DateTimeFormatter formatter;

  @Override
  public String name() {
    return "PartitionByDate";
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    String startBeginDate = prot.get("beginDate");
    String startEndDate = prot.get("endDate");
    String startPartionDay = prot.get("partionDay");
    String dateFormat = prot.get("dateFormat");
    formatter = DateTimeFormatter.ofPattern(dateFormat);
    beginDate = getTime(startBeginDate);
    endDate = 0L;
    nCount = 0;
    if (!StringUtil.isEmpty(startEndDate)) {
      endDate = getTime(startEndDate);
      nCount = (int) ((endDate - beginDate) / partionTime) + 1;
    }
    partionTime = Long.parseLong(startPartionDay) * ONE_DAY;
  }

  private long getTime(String startBeginDate) {
    try {
      return formatter.parse(startBeginDate).getLong(ChronoField.DAY_OF_YEAR);
    } catch (DateTimeParseException e) {
      throw new IllegalArgumentException(
          "columnValue:" + startBeginDate + " Please check if the format satisfied.", e);
    }
  }

  @Override
  public int calculate(String columnValue) {
    long targetTime = getTime(columnValue);
    return innerCalculate(targetTime);
  }

  private int innerCalculate(long targetTime) {
    int targetPartition = (int) ((targetTime - beginDate) / partionTime);
    if (targetTime > endDate && nCount != 0) {
      targetPartition = targetPartition % nCount;
    }
    return targetPartition;
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    long beginDate = getTime(beginValue);
    long endDate = getTime(endValue);
    ArrayList<Integer> list = new ArrayList<>();
    while (beginDate <= endDate) {
      int nodeValue = innerCalculate(beginDate);
      if (Collections.frequency(list, nodeValue) < 1) {
        list.add(nodeValue);
      }
      beginDate += ONE_DAY;
    }
    return ints(list);
  }


  @Override
  public int getPartitionNum() {
    return nCount > 0 ? nCount : -1;
  }
}
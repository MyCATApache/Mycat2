package io.mycat.router.function;

import io.mycat.router.RuleAlgorithm;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Map;

public class PartitionByLatestMonth extends RuleAlgorithm {

  private int splitOneDay;
  private int hourSpan;
  private DateTimeFormatter formatter;

  @Override
  public String name() {
    return "PartitionByLatestMonth";
  }

  @Override
  public int calculate(String columnValue) {
    TemporalAccessor date = this.formatter.parse(columnValue);
    int day = date.get(ChronoField.DAY_OF_YEAR);
    int hour = date.get(ChronoField.HOUR_OF_DAY);
    return (day - 1) * splitOneDay + hour / hourSpan;
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return -1;
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.formatter = DateTimeFormatter.ofPattern(prot.get("dateFormat"));
    this.splitOneDay = Integer.parseInt(prot.get("splitOneDay"));
    hourSpan = 24 / splitOneDay;
    if (hourSpan * 24 < 24) {
      throw new java.lang.IllegalArgumentException(
          "invalid splitOnDay param:"
              + splitOneDay
              + " should be an even number and less or equals than 24");
    }
  }
}
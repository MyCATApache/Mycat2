package io.mycat.router.function;

import io.mycat.MycatException;
import io.mycat.router.RuleFunction;
import io.mycat.router.util.StringUtil;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PartitionByMonth extends RuleFunction {

  private int partition;
  private Type type = Type.DEFAULT;
  private DateTimeFormatter formatter;
  private LocalDate beginDate;
  private LocalDate endDate;

  @Override
  public String name() {
    return "PartitionByMonth";
  }

  @Override
  public int calculate(String columnValue) {
    TemporalAccessor value = formatter.parse(columnValue);
    switch (type) {
      case DEFAULT:
        return value.get(ChronoField.MONTH_OF_YEAR) - 1;
      case UNLIMITED:
        int targetPartition = ((value.get(ChronoField.YEAR) - beginDate.getYear())
            * 12 + value.get(ChronoField.MONTH_OF_YEAR)
            - beginDate.getMonthValue());
        if (this.partition > 0) {
          targetPartition = reCalculatePartition(targetPartition);
        }
        return targetPartition;
      default:
        throw new MycatException("unsupport type");
    }
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    return doCalculateRange(beginValue, endValue, beginDate);
  }

  @Override
  public int getPartitionNum() {
    return partition;
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    String beginDateText = prot.get("beginDate");
    String endDateText = prot.get("endDate");
    String dateFormat = prot.get("dateFormat");

    formatter = DateTimeFormatter.ofPattern(dateFormat);
    LocalDate now = LocalDate.now();
    if (StringUtil.isEmpty(beginDateText) && StringUtil.isEmpty(endDateText)) {
      partition = 12;
      type = Type.DEFAULT;
      this.beginDate = now.withMonth(1);
      this.endDate = now.withMonth(12);
      return;
    } else {
      this.beginDate = LocalDate.from(formatter.parse(beginDateText));
      if (!StringUtil.isEmpty(endDateText)) {
        this.endDate = LocalDate.from(formatter.parse(endDateText));
        partition = (this.endDate.getYear() - this.beginDate.getYear()) * 12
            + endDate.getMonthValue() - beginDate.getMonthValue() + 1;
        if (this.partition <= 0) {
          throw new java.lang.IllegalArgumentException(
              "Incorrect time range for month partitioning!");
        }
      } else {
        this.partition = -1;
      }
    }
  }

  private int[] doCalculateRange(String beginValueText, String endValueText, LocalDate beginDate) {
    int startPartition = getStartPartition(beginValueText, beginDate);
    int endPartition = getEndPartition(endValueText, beginDate);
    List<Integer> list = new ArrayList<>();
    while (startPartition <= endPartition) {
      Integer nodeValue = reCalculatePartition(startPartition);
      if (Collections.frequency(list, nodeValue) < 1) {
        list.add(nodeValue);
      }
      startPartition++;
    }
    // 当在场景1： "2015-01-01", "2014-04-03" 范围出现的时候
    // 是应该返回null 还是返回 [] ?
    return ints(list);
  }

  private int getEndPartition(String endValueText, LocalDate beginDate) {
    TemporalAccessor date = formatter.parse(endValueText);
    return ((date.get(ChronoField.YEAR) - beginDate.getYear())
        * 12 + date.get(ChronoField.MONTH_OF_YEAR)
        - beginDate.getMonthValue());
  }

  private int getStartPartition(String beginValueText, LocalDate beginDate) {
    TemporalAccessor date = formatter.parse(beginValueText);
    return ((date.get(ChronoField.YEAR) - beginDate.getYear())
        * 12 + date.get(ChronoField.MONTH_OF_YEAR)
        - beginDate.getMonthValue());
  }

  /**
   * For circulatory partition, calculated value of target partition needs to be rotated to fit the
   * partition range
   */
  private int reCalculatePartition(int targetPartition) {
    /**
     * If target date is previous of start time of partition setting, shift
     * the delta range between target and start date to be positive value
     */
    if (targetPartition < 0) {
      targetPartition = this.partition - (-targetPartition) % this.partition;
    }

    if (targetPartition >= this.partition) {
      targetPartition = targetPartition % this.partition;
    }

    return targetPartition;
  }

  enum Type {
    DEFAULT, UNLIMITED,
  }
}
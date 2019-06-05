package io.mycat.router.function;

import io.mycat.router.RuleAlgorithm;
import io.mycat.router.util.StringUtil;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class PartitionByMonthAndHistory extends RuleAlgorithm {

  private DateTimeFormatter formatter;
  private LocalDate beginDate;
  private int partition;
  private LocalDate endDate;

  @Override
  public String name() {
    return "PartitionByMonthAndHistory";
  }

  @Override
  public int calculate(String columnValue) {
    TemporalAccessor value = formatter.parse(columnValue);
    int targetPartition = ((value.get(ChronoField.YEAR) - beginDate.getYear())
        * 12 + value.get(ChronoField.MONTH_OF_YEAR)
        - beginDate.getMonthValue());
    if (this.partition > 0) {
      targetPartition = reCalculatePartition(targetPartition);
    }
    return targetPartition;
  }

  @Override
  public int[] calculateRange(String beginValueText, String endValueText) {
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

  @Override
  public int getPartitionNum() {
    return partition;
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    String beginDateText = prot.get("beginDate");
    String endDateText = prot.get("endDate");
    String dateFormat = prot.get("dateFormat");

    this.formatter = DateTimeFormatter.ofPattern(dateFormat);
    this.beginDate = LocalDate.from(formatter.parse(beginDateText));

    if (!StringUtil.isEmpty(endDateText)) {
      this.endDate = LocalDate.from(formatter.parse(endDateText));
      this.partition = ((endDate.get(ChronoField.YEAR) - beginDate.getYear()) * 12
          + endDate.get(ChronoField.MONTH_OF_YEAR) - beginDate.getMonthValue()) + 1;
      if (this.partition <= 0) {
        throw new java.lang.IllegalArgumentException(
            "Incorrect time range for month partitioning!");
      }
    } else {
      this.partition = -1;
    }
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
}
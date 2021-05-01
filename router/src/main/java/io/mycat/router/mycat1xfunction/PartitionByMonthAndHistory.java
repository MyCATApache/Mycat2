/**
 * Copyright (C) <2021>  <mycat>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router.mycat1xfunction;

import io.mycat.router.CustomRuleFunction;
import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.router.util.StringUtil;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.*;

public class PartitionByMonthAndHistory extends Mycat1xSingleValueRuleFunction {

  private DateTimeFormatter formatter;
  private LocalDate beginDate;
  private int partition;
  private LocalDate endDate;

  @Override
  public String name() {
    return "PartitionByMonthAndHistory";
  }

  @Override
  public int calculateIndex( String columnValue) {
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
  public int[] calculateIndexRange(String beginValueText, String endValueText) {
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
  public void init(ShardingTableHandler table,Map<String, Object> prot, Map<String, Object> ranges) {
    String beginDateText = Objects.toString(prot.get("beginDate"));
    String endDateText = Objects.toString(prot.get("endDate"));
    String dateFormat = Objects.toString(prot.get("dateFormat"));

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
   * For circulatory partition, calculated value of targetName partition needs to be rotated to fit the
   * partition range
   */
  private int reCalculatePartition(int targetPartition) {
    /**
     * If targetName date is previous of start time of partition setting, shift
     * the delta range between targetName and start date to be positive value
     */
    if (targetPartition < 0) {
      targetPartition = this.partition - (-targetPartition) % this.partition;
    }

    if (targetPartition >= this.partition) {
      targetPartition = targetPartition % this.partition;
    }
    return targetPartition;
  }
  @Override
  public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
    if (customRuleFunction == null) return false;
    if (PartitionByMonthAndHistory.class.isAssignableFrom(customRuleFunction.getClass())) {
      PartitionByMonthAndHistory ruleFunction = (PartitionByMonthAndHistory) customRuleFunction;

      int partition = ruleFunction.partition;
      DateTimeFormatter formatter = ruleFunction.formatter;
      LocalDate beginDate = ruleFunction.beginDate;
      LocalDate endDate = ruleFunction.endDate;

      return Objects.equals(this.partition, partition)&&
              Objects.equals(this.formatter, formatter)&&
              Objects.equals(this.beginDate, beginDate)&&
              Objects.equals(this.endDate, endDate);
    }
    return false;
  }

  @Override
  public String getErUniqueID() {
    return "" + partition + formatter + beginDate + endDate;
  }
}
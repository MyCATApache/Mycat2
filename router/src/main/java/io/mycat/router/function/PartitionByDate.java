/**
 * Copyright (C) <2020>  <mycat>
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
package io.mycat.router.function;

import io.mycat.router.SingleValueRuleFunction;
import io.mycat.router.util.StringUtil;

import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

public class PartitionByDate extends SingleValueRuleFunction {

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
    partionTime = Long.parseLong(startPartionDay) * ONE_DAY;
    if (!StringUtil.isEmpty(startEndDate)) {
      endDate = getTime(startEndDate);
      nCount = (int) ((endDate - beginDate) / partionTime) + 1;
    }

  }

  private long getTime(String startBeginDate) {
    try {
      return formatter.parse(startBeginDate).getLong(ChronoField.DAY_OF_YEAR)* ONE_DAY;
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
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

import io.mycat.router.RuleFunction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.Map;

/**
 * todo check
 */
public class PartitionByHotDate extends RuleFunction {

  private long lastTime;
  private long partionTime;
  private long beginDate;
  private DateTimeFormatter formatter;

  @Override
  public String name() {
    return "PartitionByHotDate";
  }

  @Override
  public int calculate(String columnValue) {
    long targetTime = formatter.parse(columnValue).get(ChronoField.DAY_OF_YEAR);
    return innerCaculate(targetTime);
  }

  private int innerCaculate(long targetTime) {
    int targetPartition;
    long nowTime = LocalDate.now().getDayOfYear();

    beginDate = nowTime - lastTime;

    long diffDays = (nowTime - targetTime);
    if (diffDays - lastTime <= 0 || diffDays < 0) {
      targetPartition = 0;
    } else {
      targetPartition = (int) ((beginDate - targetTime) / partionTime) + 1;
    }
    return targetPartition;
  }

  @Override
  public int[] calculateRange(String beginValue, String endValue) {
    int[] targetPartition = null;
    long startTime = formatter.parse(beginValue).get(ChronoField.DAY_OF_YEAR);
    long endTime = formatter.parse(endValue).get(ChronoField.DAY_OF_YEAR);
    Calendar now = Calendar.getInstance();
    long nowTime = now.getTimeInMillis();

    long limitDate = nowTime - lastTime;
    long diffDays = (nowTime - startTime);
    if (diffDays - lastTime <= 0 || diffDays < 0) {
      int[] re = new int[1];
      targetPartition = re;
    } else {
      int[] re = null;
      int begin = 0, end = 0;
      end = this.calculate(beginValue);
      boolean hasLimit = false;
      if (endTime - limitDate > 0) {
        endTime = limitDate;
        hasLimit = true;
      }
      begin = this.innerCaculate(endTime);
      if (end >= begin) {
        int len = end - begin + 1;
        if (hasLimit) {
          re = new int[len + 1];
          re[0] = 0;
          for (int i = 0; i < len; i++) {
            re[i + 1] = begin + i;
          }
        } else {
          re = new int[len];
          for (int i = 0; i < len; i++) {
            re[i] = begin + i;
          }
        }
        return re;
      } else {
        return re;
      }
    }
    return targetPartition;
  }

  @Override
  public int getPartitionNum() {
    return -1;
  }

  @Override
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.formatter = DateTimeFormatter.ofPattern(prot.get("dateFormat"));
    this.lastTime = Integer.parseInt(prot.get("lastTime"));
    this.partionTime = Integer.parseInt(prot.get("partionTime"));
  }
}
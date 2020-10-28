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
package io.mycat.router.mycat1xfunction;

import com.google.common.hash.Hashing;
import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.ShardingTableHandler;

import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Map;
import java.util.Objects;

public class PartitionByRangeDateHash extends Mycat1xSingleValueRuleFunction {

  private DateTimeFormatter formatter;
  private long beginDate;
  private int groupPartionSize;
  private int partionDay;

  @Override
  public String name() {
    return "PartitionByRangeDateHash";
  }

  @Override
  public int calculateIndex(String columnValue) {
    long targetTime = formatter.parse(columnValue).get(ChronoField.DAY_OF_YEAR);
    int targetPartition = (int) ((targetTime - beginDate) / partionDay);
    int innerIndex = Hashing.consistentHash(targetTime, groupPartionSize);
    return targetPartition * groupPartionSize + innerIndex;
  }

  @Override
  public int[] calculateIndexRange( String beginValue, String endValue) {
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
  public void init(ShardingTableHandler table,Map<String, Object> prot, Map<String, Object> ranges) {
    this.formatter = DateTimeFormatter.ofPattern(Objects.toString(prot.get("dateFormat")));
    this.beginDate = this.formatter.parse(Objects.toString(prot.get("beginDate"))).get(ChronoField.DAY_OF_YEAR);
    this.groupPartionSize = Integer.parseInt(Objects.toString(prot.get("groupPartionSize")));
    this.partionDay = Integer.parseInt(Objects.toString(prot.get("partionDay")));
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
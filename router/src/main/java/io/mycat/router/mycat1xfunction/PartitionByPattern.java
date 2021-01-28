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

import io.mycat.router.CustomRuleFunction;
import io.mycat.router.NodeIndexRange;
import io.mycat.router.ShardingTableHandler;
import io.mycat.router.Mycat1xSingleValueRuleFunction;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public class PartitionByPattern extends Mycat1xSingleValueRuleFunction {

  private static final int PARTITION_LENGTH = 1024;
  private static final Pattern PATTERN = Pattern.compile("[0-9]*");
  private int patternValue = PARTITION_LENGTH;// 分区长度，取模数值
  private List<NodeIndexRange> longRanges;
  private int nPartition;
  private int defaultNode = 0;// 包含非数值字符，默认存储节点

  private static boolean isNumeric(String str) {
    return PATTERN.matcher(str).matches();
  }

  @Override
  public String name() {
    return "PartitionByPattern";
  }

  @Override
  public void init(ShardingTableHandler table, Map<String, Object> prot, Map<String, Object> ranges) {
    this.patternValue = Integer.parseInt(Objects.toString(prot.get("patternValue")));
    this.defaultNode = Integer.parseInt(Objects.toString(prot.get("defaultNode")));
    this.longRanges = NodeIndexRange.getLongRanges(ranges);
    this.nPartition = NodeIndexRange.getPartitionCount(this.longRanges);
  }

  @Override
  public int calculateIndex(String columnValue) {
    if (!isNumeric(columnValue)) {
      return defaultNode;
    }
    long value = Long.parseLong(columnValue);
    for (NodeIndexRange longRang : this.longRanges) {
      long hash = value % patternValue;
      if (hash <= longRang.valueEnd && hash >= longRang.valueStart) {
        return longRang.nodeIndex;
      }
    }
    return -1;
  }

  @Override
  public int[] calculateIndexRange(String beginValue, String endValue) {
    return null;
  }

  @Override
  public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
    if (customRuleFunction == null) return false;
    if (PartitionByPattern.class.isAssignableFrom(customRuleFunction.getClass())) {
      PartitionByPattern ruleFunction = (PartitionByPattern) customRuleFunction;

       int patternValue =  ruleFunction.patternValue;
       List<NodeIndexRange> longRanges = ruleFunction.longRanges;
       int nPartition = ruleFunction.nPartition;
       int defaultNode = ruleFunction.defaultNode;

      return Objects.equals(this.patternValue, patternValue)&&
              Objects.equals(this.longRanges, longRanges)&&
              Objects.equals(this.nPartition, nPartition)&&
              Objects.equals(this.defaultNode, defaultNode);
    }
    return false;
  }
  @Override
  public String getUniqueID() {
    return "" + patternValue + longRanges+nPartition+defaultNode;
  }
}
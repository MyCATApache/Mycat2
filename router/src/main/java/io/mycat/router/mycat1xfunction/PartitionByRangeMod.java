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

import java.math.BigInteger;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

public class PartitionByRangeMod extends Mycat1xSingleValueRuleFunction {

  private GroupSizeRange[] longRanges;
  private int defaultNode = -1;
  private int partitionCount;

  @Override
  public String name() {
    return "PartitionByRangeMod";
  }

  @Override
  public int calculateIndex(String columnValue) {
    long value = Long.parseLong(columnValue);
    int nodeIndex = 0;
    for (GroupSizeRange longRang : this.longRanges) {
      if (value <= longRang.valueEnd && value >= longRang.valueStart) {
        BigInteger bigNum = new BigInteger(columnValue).abs();
        int innerIndex = (bigNum.mod(BigInteger.valueOf(longRang.groupSize))).intValue();
        return nodeIndex + innerIndex;
      } else {
        nodeIndex += longRang.groupSize;
      }
    }
    // 数据超过范围，暂时使用配置的默认节点
    if (defaultNode >= 0) {
      return defaultNode;
    }
    return -1;
  }

  @Override
  public int[] calculateIndexRange(String beginValue, String endValue) {
    return null;
  }

  @Override
  public void init(ShardingTableHandler table, Map<String, Object> prot, Map<String, Object> ranges) {
    this.defaultNode = Integer.parseInt(Objects.toString(prot.get("defaultNode")));
    this.longRanges = GroupSizeRange.getGroupSizeRange(ranges);
    this.partitionCount = GroupSizeRange.getPartitionCount(this.longRanges);
  }
  @Override
  public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
    if (customRuleFunction == null) return false;
    if (PartitionByRangeMod.class.isAssignableFrom(customRuleFunction.getClass())) {
      PartitionByRangeMod ruleFunction = (PartitionByRangeMod) customRuleFunction;

       GroupSizeRange[] longRanges = ruleFunction.longRanges;
       int defaultNode = ruleFunction.defaultNode;
       int partitionCount = ruleFunction.partitionCount;

      return Arrays.equals(this.longRanges, longRanges) &&
              Objects.equals(this.defaultNode, defaultNode) &&
              Objects.equals(this.partitionCount, partitionCount);
    }
    return false;
  }
  @Override
  public String getErUniqueID() {
    return "" + Arrays.toString(longRanges) + defaultNode + partitionCount;
  }
}
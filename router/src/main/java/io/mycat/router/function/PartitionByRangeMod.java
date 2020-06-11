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

import io.mycat.router.ShardingTableHandler;
import io.mycat.router.SingleValueRuleFunction;

import java.math.BigInteger;
import java.util.Map;

public class PartitionByRangeMod extends SingleValueRuleFunction {

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
  public int getPartitionNum() {
    return partitionCount;
  }

  @Override
  public void init(ShardingTableHandler table, Map<String, String> prot, Map<String, String> ranges) {
    this.defaultNode = Integer.parseInt(prot.get("defaultNode"));
    this.longRanges = GroupSizeRange.getGroupSizeRange(ranges);
    this.partitionCount = GroupSizeRange.getPartitionCount(this.longRanges);
  }
}
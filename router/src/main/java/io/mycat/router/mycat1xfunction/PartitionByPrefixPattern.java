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

import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.NodeIndexRange;
import io.mycat.router.ShardingTableHandler;

import java.util.List;
import java.util.Map;

public class PartitionByPrefixPattern extends Mycat1xSingleValueRuleFunction {

  private static final int PARTITION_LENGTH = 1024;
  private int patternValue = PARTITION_LENGTH;// 分区长度，取模数值(默认为1024)
  private int prefixLength;// 字符前几位进行ASCII码取和
  private List<NodeIndexRange> longRongs;
  private int nPartition;

  @Override
  public String name() {
    return "PartitionByPrefixPattern";
  }

  @Override
  public void init(ShardingTableHandler table, Map<String, String> prot, Map<String, String> ranges) {
    this.patternValue = Integer.parseInt(prot.get("patternValue"));
    this.prefixLength = Integer.parseInt(prot.get("prefixLength"));
    this.longRongs = NodeIndexRange.getLongRanges(ranges);
    this.nPartition = NodeIndexRange.getPartitionCount(this.longRongs);
  }

  @Override
  public int calculateIndex(String columnValue) {
    int length = Math.min(columnValue.length(), prefixLength);
    int sum = 0;
    for (int i = 0; i < length; i++) {
      sum = sum + columnValue.charAt(i);
    }
    for (NodeIndexRange longRang : this.longRongs) {
      long hash = sum % patternValue;
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


}
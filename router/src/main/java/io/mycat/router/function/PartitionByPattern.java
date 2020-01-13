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

import io.mycat.router.NodeIndexRange;
import io.mycat.router.RuleFunction;

import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class PartitionByPattern extends RuleFunction {

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
  public void init(Map<String, String> prot, Map<String, String> ranges) {
    this.patternValue = Integer.parseInt(prot.get("patternValue"));
    this.defaultNode = Integer.parseInt(prot.get("defaultNode"));
    this.longRanges = NodeIndexRange.getLongRanges(ranges);
    this.nPartition = NodeIndexRange.getPartitionCount(this.longRanges);
  }

  @Override
  public int calculate(String columnValue) {
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
  public int[] calculateRange(String beginValue, String endValue) {
    return calculateSequenceRange(this, beginValue, endValue);
  }

  @Override
  public int getPartitionNum() {
    return nPartition;
  }
}
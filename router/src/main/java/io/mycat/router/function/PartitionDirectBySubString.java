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

import java.util.Map;

public class PartitionDirectBySubString extends SingleValueRuleFunction {

  // 字符子串起始索引（zero-based)
  private int startIndex;
  // 字串长度
  private int size;
  // 分区数量
  private int partitionCount;
  // 默认分区（在分区数量定义时，字串标示的分区编号不在分区数量内时，使用默认分区）
  private int defaultNode;

  @Override
  public String name() {
    return "PartitionDirectBySubString";
  }

  @Override
  public void init(ShardingTableHandler table, Map<String, String> prot, Map<String, String> ranges) {
    this.startIndex = Integer.parseInt(prot.get("startIndex"));
    this.size = Integer.parseInt(prot.get("size"));
    this.partitionCount = Integer.parseInt(prot.get("partitionCount"));
    this.defaultNode = Integer.parseInt(prot.get("defaultNode"));
  }

  @Override
  public int calculateIndex(String columnValue) {
    String partitionSubString = columnValue.substring(startIndex, startIndex + size);
    int partition = Integer.parseInt(partitionSubString, 10);
    return partitionCount > 0 && partition >= partitionCount
        ? defaultNode : partition;
  }

  @Override
  public int[] calculateIndexRange(String beginValue, String endValue) {
    return null;
  }

}
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

import java.util.Map;
import java.util.Objects;

public class PartitionDirectBySubString extends Mycat1xSingleValueRuleFunction {

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
    public void init(ShardingTableHandler table, Map<String, Object> prot, Map<String, Object> ranges) {
        this.startIndex = Integer.parseInt(Objects.toString(prot.get("startIndex")));
        this.size = Integer.parseInt(Objects.toString(prot.get("size")));
        this.partitionCount = Integer.parseInt(Objects.toString(prot.get("partitionCount")));
        this.defaultNode = Integer.parseInt(Objects.toString(prot.get("defaultNode")));
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

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (PartitionDirectBySubString.class.isAssignableFrom(customRuleFunction.getClass())) {
            PartitionDirectBySubString ruleFunction = (PartitionDirectBySubString) customRuleFunction;

            int startIndex = ruleFunction.startIndex;
            int size = ruleFunction.size;
            int partitionCount = ruleFunction.partitionCount;
            int defaultNode = ruleFunction.defaultNode;
            return Objects.equals(this.startIndex, startIndex) &&
                    Objects.equals(this.size, size) &&
                    Objects.equals(this.partitionCount, partitionCount) &&
                    Objects.equals(this.defaultNode, defaultNode);

        }
        return false;
    }

    @Override
    public String getErUniqueID() {
        return  getClass().getName()+":"+ startIndex + size + partitionCount + defaultNode;
    }

}
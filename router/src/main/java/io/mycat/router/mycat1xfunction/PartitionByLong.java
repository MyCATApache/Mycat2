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
import io.mycat.router.Mycat1xSingleValueRuleFunction;
import io.mycat.router.ShardingTableHandler;
import io.mycat.router.util.PartitionUtil;

import java.util.Map;
import java.util.Objects;

/**
 * @author jamie12221 date 2019-05-02 23:36
 **/
public class PartitionByLong extends Mycat1xSingleValueRuleFunction {

    private PartitionUtil partitionUtil;

    @Override
    public String name() {
        return "PartitionByLong";
    }

    @Override
    public void init(ShardingTableHandler table, Map<String, Object> properties, Map<String, Object> ranges) {
        int[] count = toIntArray(Objects.toString(properties.get("partitionCount")));
        int[] length = toIntArray(Objects.toString(properties.get("partitionLength")));
        partitionUtil = new PartitionUtil(count, length);
    }

    @Override
    public int calculateIndex(String columnValue) {
        try {
            long key = Long.parseLong(columnValue);
            key = (key >>> 32) ^ key;
            return partitionUtil.partition(key);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "columnValue:" + columnValue + " Please eliminate any quote and non number within it.",
                    e);
        }
    }

    @Override
    public int[] calculateIndexRange(String beginValue, String endValue) {
        return null;
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (PartitionByLong.class.isAssignableFrom(customRuleFunction.getClass())) {
            PartitionByLong ruleFunction = (PartitionByLong) customRuleFunction;
            PartitionUtil partitionUtil = ruleFunction.partitionUtil;
            return Objects.equals(this.partitionUtil, partitionUtil);
        }
        return false;
    }

    @Override
    public String getErUniqueID() {
        return "" + partitionUtil;
    }
}

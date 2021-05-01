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
import java.util.Map;
import java.util.Objects;

public class PartitionByMod extends Mycat1xSingleValueRuleFunction {

    private BigInteger count;

    @Override
    public String name() {
        return "PartitionByMod";
    }

    @Override
    public void init(ShardingTableHandler table, Map<String, Object> prot, Map<String, Object> ranges) {
        String count = Objects.toString(prot.get("count"));
        Objects.requireNonNull(count);
        this.count = new BigInteger(count);
    }

    @Override
    public int calculateIndex(String columnValue) {
        try {
            BigInteger bigNum = new BigInteger(columnValue).abs();
            return (bigNum.mod(count)).intValue();
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
        if (PartitionByMod.class.isAssignableFrom(customRuleFunction.getClass())) {
            PartitionByMod ruleFunction = (PartitionByMod) customRuleFunction;
            return Objects.equals(this.count, ruleFunction.count);
        }
        return false;
    }

    @Override
    public String getErUniqueID() {
        return "" + count;
    }
}
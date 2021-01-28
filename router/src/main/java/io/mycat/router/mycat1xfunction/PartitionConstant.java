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

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

/**
 * @author jamie12221 date 2020-01-04
 **/
public class PartitionConstant extends Mycat1xSingleValueRuleFunction {

    private int defaultNode;
    private int[] nodes;

    @Override
    public String name() {
        return "PartitionConstant";
    }

    @Override
    public void init(ShardingTableHandler table, Map<String, Object> properties, Map<String, Object> ranges) {
        this.defaultNode = Integer.parseInt(Objects.toString(properties.get("defaultNode")));
        this.nodes = new int[]{defaultNode};
    }

    @Override
    public int calculateIndex(String columnValue) {
        return defaultNode;
    }

    @Override
    public int[] calculateIndexRange(String beginValue, String endValue) {
        return nodes;
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (PartitionConstant.class.isAssignableFrom(customRuleFunction.getClass())) {
            PartitionConstant ruleFunction = (PartitionConstant) customRuleFunction;

             int defaultNode = ruleFunction.defaultNode;
             int[] nodes = ruleFunction.nodes;

            return Objects.equals(this.defaultNode, defaultNode) &&
                    Arrays.equals(this.nodes, nodes);
        }
        return false;
    }
    @Override
    public String getUniqueID() {
        return "" + defaultNode + Arrays.toString(nodes);
    }
}

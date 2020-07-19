/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt4.physical.rules;

import com.google.common.collect.ImmutableList;
import io.mycat.hbt4.MycatConvention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.List;

public class MycatPhysicalRules {
    public static List<RelOptRule> rules(MycatConvention out) {
        return rules(out, RelFactories.LOGICAL_BUILDER);
    }
    public static List<RelOptRule> rules(MycatConvention out,
                                         RelBuilderFactory relBuilderFactory) {
        return ImmutableList.of(
                new HashAggRule(out, relBuilderFactory),
                new HashJoinRule(out, relBuilderFactory),
                new MaterializedSemiJoinRule(out, relBuilderFactory),
                new MemSortRule(out, relBuilderFactory),
                new MergeSortRule(out, relBuilderFactory),
                new NestedLoopJoinRule(out, relBuilderFactory),
                new SemiHashJoinRule(out, relBuilderFactory),
                new MycatSortAggRule(out, relBuilderFactory),
                new SortMergeJoinRule(out, relBuilderFactory),
                new SortMergeSemiJoinRule(out, relBuilderFactory),
                new TopNRule(out, relBuilderFactory)
        );
    }
}
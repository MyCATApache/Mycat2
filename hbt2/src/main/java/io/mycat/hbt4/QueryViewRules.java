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
package io.mycat.hbt4;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.rules.*;


/**
 * 搜索以SetOpTransposeRule结尾
 * UnionTransposeRule结尾
 */
public class QueryViewRules {
   public static ImmutableList RULES = ImmutableList.of(
           SortUnionTransposeRule.INSTANCE,
           FilterSetOpTransposeRule.INSTANCE,//Filter上拉集合操作
           AggregateUnionTransposeRule.INSTANCE,//聚合上拉集合操作
           JoinUnionTransposeRule.LEFT_UNION,//join上拉集合操作
           JoinUnionTransposeRule.RIGHT_UNION,
           ProjectSetOpTransposeRule.INSTANCE,//project上拉集合操作
           UnionEliminatorRule.INSTANCE,
           UnionMergeRule.INSTANCE
//           UnionPullUpConstantsRule.INSTANCE,//有问题
//           UnionToDistinctRule.INSTANCE,//此规则没有必要
   );
}
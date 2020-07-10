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
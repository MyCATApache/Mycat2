/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router;

import io.mycat.beans.mycat.MycatTable;
import java.util.List;
import java.util.Map;

/**
 * @author mycat
 * @author cjw
 * 路由算法接口
 */
public abstract class RuleAlgorithm {

  List<RuleAlgorithm> nextAlgorithm;

  public abstract String name();

  public static int[] toIntArray(String string) {
    String[] strs = io.mycat.util.SplitUtil.split(string, ',', true);
    int[] ints = new int[strs.length];
    for (int i = 0; i < strs.length; ++i) {
      ints[i] = Integer.parseInt(strs[i]);
    }
    return ints;
  }

  /**
   * return sharding nodes's id columnValue is column's value
   *
   * @return never null
   */
  public abstract int calculate(String columnValue);

  public abstract int[] calculateRange(String beginValue, String endValue);

  /**
   * 分片表所跨的节点数与分片算法分区数一致性校验
   *
   * @return -1 if table datanode size < rule function partition size 0 if table datanode size ==
   * rule function partition size 1 if table datanode size > rule function partition size
   */
  public int suitableFor(MycatTable tableConf) {
    int nPartition = getPartitionNum();
    if (nPartition > 0) { // 对于有限制分区数的规则,进行检查
      int dnSize = tableConf.getDataNodes().size();
      if (dnSize < nPartition) {
        return -1;
      } else if (dnSize > nPartition) {
        return 1;
      }
    }
    return 0;
  }

  /**
   * 对于存储数据按顺序存放的字段做范围路由，可以使用这个函数
   */
  public static int[] calculateSequenceRange(RuleAlgorithm algorithm, String beginValue,
      String endValue) {
    int begin = 0, end = 0;
    begin = algorithm.calculate(beginValue);
    end = algorithm.calculate(endValue);
    if (end >= begin) {
      int len = end - begin + 1;
      int[] re = new int[len];
      for (int i = 0; i < len; i++) {
        re[i] = begin + i;
      }
      return re;
    } else {
      return new int[0];
    }
  }

  public abstract int getPartitionNum();

  public List<RuleAlgorithm> getSubRuleAlgorithm() {
    return nextAlgorithm;
  }

  public void setSubRuleAlgorithm(List<RuleAlgorithm> ruleAlgorithm) {
    nextAlgorithm = ruleAlgorithm;
  }

  /**
   * init
   */
  public abstract void init(Map<String, String> prot, Map<String, String> ranges);

  protected static int[] ints(List<Integer> list) {
    int[] ints = new int[list.size()];
    for (int i = 0; i < ints.length; i++) {
      ints[i] = list.get(i);
    }
    return ints;
  }

}
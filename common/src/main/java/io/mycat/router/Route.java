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

import io.mycat.MycatExpection;

/**
 * @author jamie12221
 * @date 2019-05-03 18:09
 * 分片多级路由处理器
 **/
public class Route {

  private final String column;
  private final String equalKey;
  private final String rangeStart;
  private final String rangeEnd;
  Route nextRoute;


  public Route(String column, String equalKey, String rangeStart, String rangeEnd) {
    this.equalKey = equalKey;
    this.rangeStart = rangeStart;
    this.rangeEnd = rangeEnd;
    this.column = column;
  }

  public interface RouteIndexReceive {

    void addDataNodeIndex(int level,RuleAlgorithm ruleAlgorithm,int index);

    void addDataNodeIndexes(int level,RuleAlgorithm ruleAlgorithm,int[] index);
  }

  public void route(DynamicAnnotationResult result,int level, RuleAlgorithm ruleAlgorithm,
      RouteIndexReceive receive) {
    String equal = result.get(equalKey);
    String columnStart = result.get(rangeStart);
    String columnEnd = result.get(rangeEnd);
    if (equal != null && columnStart != null) {
      throw new MycatExpection("");
    } else if (equal == null && columnStart == null) {
      throw new MycatExpection("");
    } else if (equal != null && columnStart == null) {
      int nodeIndex = ruleAlgorithm.calculate(equal);
      if (nextRoute != null) {
        RuleAlgorithm ruleAlgorithm1 = ruleAlgorithm.getSubRuleAlgorithm().get(nodeIndex);
        nextRoute.route(result,level+1, ruleAlgorithm1, receive);
        return;
      } else {
        if (ruleAlgorithm.getSubRuleAlgorithm() == null) {
          receive.addDataNodeIndex(nodeIndex,ruleAlgorithm,nodeIndex);
          return;
        } else {
          throw new MycatExpection("");
        }
      }
    } else if (equal == null && columnStart != null && columnEnd != null) {
      int[] nodeIndexes = ruleAlgorithm.calculateRange(columnStart, columnEnd);
      if (nextRoute != null) {
        for (int nodeIndex : nodeIndexes) {
          RuleAlgorithm ruleAlgorithm1 = ruleAlgorithm.getSubRuleAlgorithm().get(nodeIndex);
          nextRoute.route(result,level+1, ruleAlgorithm1, receive);
        }
        return;
      } else {
        if (ruleAlgorithm.getSubRuleAlgorithm() == null) {
          receive.addDataNodeIndexes(level+1,ruleAlgorithm,nodeIndexes);
        } else {
          throw new MycatExpection("");
        }
      }
    }
  }

  public String getColumn() {
    return column;
  }

  public String getEqualKey() {
    return equalKey;
  }

  public String getRangeStart() {
    return rangeStart;
  }

  public String getRangeEnd() {
    return rangeEnd;
  }


  public Route getNextRoute() {
    return nextRoute;
  }


  public void setNextRoute(Route nextRoute) {
    this.nextRoute = nextRoute;
  }
}

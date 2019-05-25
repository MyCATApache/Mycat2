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
import java.util.Objects;
import java.util.Set;

/**
 * @author jamie12221
 *  date 2019-05-03 18:09 分片多级路由处理器
 **/
public class Route {

  private final String column;
  private final Set<String> equalKeys;
  private final Set<String> rangeStartKeys;
  private final Set<String> rangeEndKeys;
  Route nextRoute;


  public Route(String column, Set<String> equalKey, Set<String> rangeStart, Set<String> rangeEnd) {
    this.equalKeys = equalKey;
    this.rangeStartKeys = rangeStart;
    this.rangeEndKeys = rangeEnd;
    this.column = column;
  }

  public void route(DynamicAnnotationResult result, int level, RuleAlgorithm ruleAlgorithm,
      RouteIndexReceive receive) {
    String equal = null;
    for (String equalKey : equalKeys) {
      equal = result.get(equalKey);
      if (equal != null) {
        break;
      }
    }
    String columnStart = null;
    String columnEnd = null;
    for (String rangeStartKey : rangeStartKeys) {
      columnStart = result.get(rangeStartKey);
      if (columnStart != null) {
        break;
      }
    }
    for (String rangeEndKey : rangeEndKeys) {
      columnEnd = result.get(rangeEndKey);
      if (columnEnd != null) {
        break;
      }
    }
    if (equal != null && columnStart != null) {
      throw new MycatExpection(result.getSQL() + "can not get equal key or rangeKey");
    } else if (equal == null && columnStart == null) {
      expection(equal, columnStart, columnEnd);
      return;
    } else if (equal != null && columnStart == null) {
      int nodeIndex = ruleAlgorithm.calculate(equal);
      if (nextRoute != null) {
        RuleAlgorithm ruleAlgorithm1 = ruleAlgorithm.getSubRuleAlgorithm().get(nodeIndex);
        nextRoute.route(result, level + 1, ruleAlgorithm1, receive);
        return;
      } else {
        if (ruleAlgorithm.getSubRuleAlgorithm() == null) {
          receive.addDataNodeIndex(nodeIndex, ruleAlgorithm, nodeIndex);
          return;
        } else {
          throw new MycatExpection("error table rule");
        }
      }
    } else if (equal == null && columnStart != null && columnEnd != null) {
      int[] nodeIndexes = ruleAlgorithm.calculateRange(columnStart, columnEnd);
      if (nextRoute != null) {
        for (int nodeIndex : nodeIndexes) {
          RuleAlgorithm ruleAlgorithm1 = ruleAlgorithm.getSubRuleAlgorithm().get(nodeIndex);
          nextRoute.route(result, level + 1, ruleAlgorithm1, receive);
        }
        return;
      } else {
        if (ruleAlgorithm.getSubRuleAlgorithm() == null) {
          receive.addDataNodeIndexes(level + 1, ruleAlgorithm, nodeIndexes);
          if (nodeIndexes.length == 1) {
            receive.addDataNodeIndex(level + 1, ruleAlgorithm, nodeIndexes[0]);
          }
        } else {
          throw new MycatExpection(result.getSQL() + "meet false tableRule");
        }
      }
    }
  }

  public void expection(String equal, String columnStart, String columnEnd) {
    throw new MycatExpection(String
                                 .format("sql:%s equal:%s start:{} end:%s", Objects.toString(equal),
                                     Objects.toString(columnStart),
                                     Objects.toString(columnEnd)));
  }

  public interface RouteIndexReceive {

    void addDataNodeIndex(int level, RuleAlgorithm ruleAlgorithm, int index);

    void addDataNodeIndexes(int level, RuleAlgorithm ruleAlgorithm, int[] index);
  }

  public String getColumn() {
    return column;
  }



  public void setNextRoute(Route nextRoute) {
    this.nextRoute = nextRoute;
  }
}

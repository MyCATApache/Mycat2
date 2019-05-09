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
package io.mycat.beans.mycat;

import io.mycat.router.DynamicAnnotationMatcher;
import io.mycat.router.DynamicAnnotationResult;
import io.mycat.router.Route;
import io.mycat.router.RuleAlgorithm;

/**
 * @author jamie12221
 * @date 2019-05-05 13:16
 **/
public class MycatTableRule {

  public MycatTableRule(String name, Route route, RuleAlgorithm ruleAlgorithm,
      DynamicAnnotationMatcher matcher) {
    this.ruleAlgorithm = ruleAlgorithm;
    this.name = name;
    this.route = route;
    this.matcher = matcher;
  }

  final RuleAlgorithm ruleAlgorithm;
  final String name;
  final Route route;
  final DynamicAnnotationMatcher matcher;

  public RuleAlgorithm getRuleAlgorithm() {
    return ruleAlgorithm;
  }

  public String getName() {
    return name;
  }

  public Route getRoute() {
    return route;
  }

  public DynamicAnnotationMatcher getMatcher() {
    return matcher;
  }

}

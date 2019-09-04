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

import io.mycat.router.routeStrategy.SqlParseRouteRouteStrategy;
import io.mycat.sqlparser.util.simpleParser.BufferSQLContext;
import java.util.Map;

/**
 * @author jamie12221
 *  date 2019-05-04 02:17
 **/
public class RouteContext implements Route.RouteIndexReceive {

  Map<String, String> result;
  RuleAlgorithm ruleAlgorithm;
  BufferSQLContext sqlContext;
  SqlParseRouteRouteStrategy sqlParseRouteRouteStrategy = new SqlParseRouteRouteStrategy();
  int[] indexes;
  int index;
  MycatRouterConfig config;
  final MycatProxyStaticAnnotation staticAnnotation = new MycatProxyStaticAnnotation();

  public RouteContext(MycatRouterConfig config) {
    this.config = config;
  }

  public MycatRouterConfig getConfig() {
    return config;
  }

  public BufferSQLContext getSqlContext() {
    return sqlContext;
  }

  public void setSqlContext(BufferSQLContext sqlContext) {
    this.sqlContext = sqlContext;
  }

  public void clear() {
    this.index = -1;
    this.indexes = null;
    staticAnnotation.clear();
 }

  public int[] getIndexes() {
    return indexes;
  }

  public int getIndex() {
    return index;
  }

  public SqlParseRouteRouteStrategy getSqlParseRouteRouteStrategy() {
    return sqlParseRouteRouteStrategy;
  }



  public Map<String, String> getResult() {
    return result;
  }

  public void setResult(Map<String, String> result) {
    this.result = result;
  }

  public RuleAlgorithm getRuleAlgorithm() {
    return ruleAlgorithm;
  }

  public void setRuleAlgorithm(RuleAlgorithm ruleAlgorithm) {
    this.ruleAlgorithm = ruleAlgorithm;
  }

  @Override
  public void addDataNodeIndex(int level, RuleAlgorithm ruleAlgorithm, int index) {
    this.index = index;
  }

  @Override
  public void addDataNodeIndexes(int level, RuleAlgorithm ruleAlgorithm, int[] index) {
    this.indexes = indexes;
  }

  /**
   * Getter for property 'staticAnnotation'.
   *
   * @return Value for property 'staticAnnotation'.
   */
  public MycatProxyStaticAnnotation getStaticAnnotation() {
    return staticAnnotation;
  }

}

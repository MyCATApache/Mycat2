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

/**
 * @author jamie12221
 *  date 2019-05-02 23:51
 **/
public interface  ShardingAlgorithmAspect<T> {

//  Map<Integer, List<T>> nodeMap = new HashMap<>();

   <A> void batchInsert(A ast);

   <A> void migrate(A ast);

  <A> void routeByERParentKey(A ast);

  <A> void ruteByERParentKey(A ast);
  <A> void tryRouteForOneTable(A ast);
  <A> void routeToDistTableNode(A ast);
  <A> void routeToMultiNode(A ast);
  <A> void  findRouteWithcConditionsForTables(A ast);
}

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
package io.mycat.router.routeResult;

import io.mycat.router.ResultRoute;
import java.util.Arrays;
import java.util.Objects;

/**
 * @author jamie12221
 * @date 2019-05-05 19:25
 * 全局表路由结果
 **/
public class GlobalTableWriteResultRoute extends ResultRoute {
  CharSequence sql;
  String[] dataNodes;

  public CharSequence getSql() {
    return sql;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GlobalTableWriteResultRoute that = (GlobalTableWriteResultRoute) o;
    return Objects.equals(sql, that.sql) &&
               Arrays.equals(dataNodes, that.dataNodes);
  }

  @Override
  public int hashCode() {
    int result = Objects.hash(sql);
    result = 31 * result + Arrays.hashCode(dataNodes);
    return result;
  }

  @Override
  public String toString() {
    return "GlobalTableWriteResultRoute{" +
               "sql=" + sql +
               ", dataNodes=" + Arrays.toString(dataNodes) +
               '}';
  }
  @Override
  public <CONTEXT> void accept(Executer<CONTEXT> executer,CONTEXT context) {
    executer.run(this,context);
  }


  public void setSql(CharSequence sql) {
    this.sql = sql;
  }

  public String[] getDataNodes() {
    return dataNodes;
  }

  public void setDataNodes(String[] dataNodes) {
    this.dataNodes = dataNodes;
  }
}

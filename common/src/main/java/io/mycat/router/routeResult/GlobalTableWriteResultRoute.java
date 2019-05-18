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
import java.util.Collection;

/**
 * @author jamie12221
 * @date 2019-05-05 19:25
 * 全局表路由结果
 **/
public class GlobalTableWriteResultRoute extends ResultRoute {

  String sql;
  Collection<String> dataNodes;
  String master;

  public String getMaster() {
    return master;
  }

  public GlobalTableWriteResultRoute setMaster(String master) {
    this.master = master;
    return this;
  }

  public String getSql() {
    return sql;
  }

  public GlobalTableWriteResultRoute setSql(String sql) {
    this.sql = sql;
    return this;
  }

  @Override
  public <CONTEXT> void accept(Executer<CONTEXT> executer, CONTEXT context) {
    executer.run(this, context);
  }

  @Override
  public ResultRouteType getType() {
    return ResultRouteType.GLOBAL_TABLE_WRITE_RESULT_ROUTE;
  }

  public Collection<String> getDataNodes() {
    return dataNodes;
  }

  public GlobalTableWriteResultRoute setDataNodes(Collection<String> dataNodes) {
    this.dataNodes = dataNodes;
    return this;
  }

  @Override
  public String toString() {
    return "GlobalTableWriteResultRoute{" +
               "sql='" + sql + '\'' +
               ", dataNodes=" + dataNodes +
               ", master='" + master + '\'' +
               '}';
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

    if (sql != null ? !sql.equals(that.sql) : that.sql != null) {
      return false;
    }
    if (dataNodes != null ? !dataNodes.equals(that.dataNodes) : that.dataNodes != null) {
      return false;
    }
    return master != null ? master.equals(that.master) : that.master == null;
  }

  @Override
  public int hashCode() {
    int result = sql != null ? sql.hashCode() : 0;
    result = 31 * result + (dataNodes != null ? dataNodes.hashCode() : 0);
    result = 31 * result + (master != null ? master.hashCode() : 0);
    return result;
  }
}

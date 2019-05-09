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
import java.io.IOException;
import java.util.Objects;

/**
 * @author jamie12221
 * @date 2019-05-05 12:54
 **/

public class OneServerResultRoute extends ResultRoute {
  String dataNode;
  CharSequence sql;
  public String getDataNode() {
    return dataNode;
  }

  public OneServerResultRoute setDataNode(String dataNode) {
    this.dataNode = dataNode;
    return this;
  }

  public CharSequence getSql() {
    return sql;
  }

  public OneServerResultRoute setSql(CharSequence sql) {
    this.sql = sql;
    return this;
  }

  @Override
  public String toString() {
    return "OneServerResultRoute{" +
               "dataNode='" + dataNode + '\'' +
               ", sql=" + sql +
               '}';
  }

  @Override
  public <CONTEXT> void accept(Executer<CONTEXT> executer,CONTEXT context) throws IOException {
    executer.run(this,context);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    OneServerResultRoute result = (OneServerResultRoute) o;
    return Objects.equals(dataNode, result.dataNode) &&
               Objects.equals(sql, result.sql);
  }

  @Override
  public int hashCode() {
    return Objects.hash(dataNode, sql);
  }
}

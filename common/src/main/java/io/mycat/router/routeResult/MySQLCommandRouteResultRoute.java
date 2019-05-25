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
import java.util.Objects;

/**
 * @author jamie12221
 *  date 2019-05-07 13:26
 * 非sql命令路由结果
 **/
public class MySQLCommandRouteResultRoute extends ResultRoute {
  byte cmd;
  String dataNode;

  public byte getCmd() {
    return cmd;
  }

  public void setCmd(int cmd) {
    this.cmd = (byte) cmd;
  }

  public String getDataNode() {
    return dataNode;
  }

  public void setDataNode(String dataNode) {
    this.dataNode = dataNode;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MySQLCommandRouteResultRoute that = (MySQLCommandRouteResultRoute) o;
    return cmd == that.cmd &&
               Objects.equals(dataNode, that.dataNode);
  }

  @Override
  public int hashCode() {
    return Objects.hash(cmd, dataNode);
  }
  @Override
  public <CONTEXT> void accept(Executer<CONTEXT> executer,CONTEXT context) {
    executer.run(this,context);
  }

  @Override
  public ResultRouteType getType() {
    return null;
  }


  @Override
  public String toString() {
    return "MySQLCommandRouteResultRoute{" +
               "cmd=" + cmd +
               ", dataNode='" + dataNode + '\'' +
               '}';
  }
}

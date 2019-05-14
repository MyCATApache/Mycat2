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

import io.mycat.router.routeResult.GlobalTableWriteResultRoute;
import io.mycat.router.routeResult.MySQLCommandRouteResultRoute;
import io.mycat.router.routeResult.OneServerResultRoute;
import io.mycat.router.routeResult.ResultRouteType;
import io.mycat.router.routeResult.SubTableResultRoute;
import io.mycat.router.routeResult.dbResultSet.DbResultSet;
import java.io.IOException;

/**
 * @author jamie12221
 * @date 2019-05-05 13:33 路由结果接口
 **/
public abstract class ResultRoute {

  public abstract boolean equals(Object o);

  public abstract int hashCode();

  public abstract String toString();

  public abstract <CONTEXT> void accept(Executer<CONTEXT> executer,CONTEXT context)
      throws IOException;

  public abstract ResultRouteType getType();

  public interface Executer <CONTEXT>{
    void run(DbResultSet dbResultSet,CONTEXT context);
    void run(OneServerResultRoute dbResultSet,CONTEXT context) throws IOException;
    void run(GlobalTableWriteResultRoute dbResultSet,CONTEXT context);
    void run(MySQLCommandRouteResultRoute dbResultSet,CONTEXT context);
    void run(SubTableResultRoute dbResultSet,CONTEXT context);
  }
}

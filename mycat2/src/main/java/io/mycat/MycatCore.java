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
package io.mycat;

import io.mycat.proxy.ProxyRuntime;
import io.mycat.replica.DefaultMySQLReplicaFactory;
import io.mycat.replica.MySQLDataSourceEx;
import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author cjw
 **/
public class MycatCore {


  public static void main(String[] args) throws Exception {
    ProxyRuntime runtime = ProxyRuntime.INSTANCE;
    runtime.loadMycat();
    runtime.loadProxy();
    runtime.initReactor(MycatCommandHandler::new);
    runtime.initRepliac(new DefaultMySQLReplicaFactory());
    runtime.initAcceptor();

    ScheduledExecutorService service = Executors.newScheduledThreadPool(1);
    service.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        Collection<MySQLDataSourceEx> datasourceList = runtime.getMySQLDatasourceList();
        for (MySQLDataSourceEx datasource : datasourceList) {
          datasource.heartBeat();
        }
      }
    }, 0, 3, TimeUnit.SECONDS);
  }

}

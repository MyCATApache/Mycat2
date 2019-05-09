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

import io.mycat.proxy.executer.MySQLDataNodeExecuter;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.MycatRuntime;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.task.AsynTaskCallBack;
import io.mycat.proxy.task.AsynTaskFuture;

/**
 * @author cjw
 **/
public class MycatCore {


  public static void main(String[] args) throws Exception {
    MycatRuntime runtime = MycatRuntime.INSTANCE;
    runtime.loadMycat();
    runtime.loadProxy();
    runtime.initReactor();
    runtime.initRepliac();
    runtime.initHeartbeat();
    runtime.initAcceptor();



    //clear(runtime);

  }

  private static void init(MycatRuntime runtime) {
    AsynTaskFuture<MySQLClientSession> future = AsynTaskFuture.future();
     MySQLDataNodeExecuter.getMySQLSessionFromUserThread("dn1",MySQLIsolation.READ_UNCOMMITTED, MySQLAutoCommit.OFF, "UTF8",
        false, null, future);

    future.setCallBack(new AsynTaskCallBack<MySQLClientSession>() {
      @Override
      public void finished(MySQLClientSession m, Object sender, boolean success, Object result,
          Object errorMessage) {
        m.ping( new AsynTaskCallBack<MySQLClientSession>() {
          @Override
          public void finished(MySQLClientSession session, Object sender, boolean success, Object result,
              Object attr) {
            if (success) {
              System.out.print("ssss");
            }
            System.out.print("ssss");
          }
        });

      }
    });
//
//    future.setCallBack(new AsynTaskCallBack<MySQLClientSession>() {
//      @Override
//      public void finished(MySQLClientSession session, Object sender, boolean success, Object result,
//          Object errorMessage) {
//        session.loadData("LOAD DATA LOCAL INFILE 'd:/loadData.csv' INTO TABLE test;",
//            new AsynTaskCallBack<MySQLClientSession>() {
//              @Override
//              public void finished(MySQLClientSession session, Object sender, boolean success,
//                  Object result, Object errorMessage) {
//                session.commit(new AsynTaskCallBack<MySQLClientSession>() {
//                  @Override
//                  public void finished(MySQLClientSession session, Object sender, boolean success,
//                      Object result, Object errorMessage) {
//                  }
//                });
//              }
//            });
//      }
//    });
//    future.setCallBack((session, sender, success, result, errorMessage) -> {
//      AsynTaskFuture<MySQLClientSession> prepare = session.prepare(
//          "INSERT INTO `db1`.`test` (`2`) VALUES (?);");
//      prepare.setCallBack((session1, sender1, success1, result1, errorMessage1) -> {
//        MySQLPreparedStatement ps = (MySQLPreparedStatement) result1;
//        session1.sendBlob(ps, 0, "hello".getBytes())
//            .setCallBack((session13, sender13, success13, result13, errorMessage13) -> {
//              AsynTaskFuture<MySQLClientSession> execute = session13.execute(ps,
//                  MySQLPrepareStmtExecuteFlag.CURSOR_TYPE_NO_CURSOR,
//                  ResultSetCollectorImpl.INSTANCE);
//              execute.setCallBack(
//                  (session2, sender2, success2, result2, errorMessage2) -> {
//                    session2.commit()
//                        .setCallBack(
//                            (session21, sender21, success21, result21, errorMessage21) -> {
//                              session21
//                                  .close(ps,
//                                      (session3, sender3, success3, result3, errorMessage3) -> {
//                                        session3
//                                            .doQuery(
//                                                "select 1;")
//                                            .setCallBack(
//                                                new AsynTaskCallBack<MySQLClientSession>() {
//                                                  @Override
//                                                  public void finished(
//                                                      MySQLClientSession session,
//                                                      Object sender,
//                                                      boolean success,
//                                                      Object result,
//                                                      Object errorMessage) {
//                                                  }
//                                                });
//                                      });
//                            });
//                  });
//            });
//      });
//    });
  }

}

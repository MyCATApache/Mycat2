package io.mycat.command;

import static io.mycat.sqlparser.util.BufferSQLContext.DESCRIBE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_AUTOCOMMIT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_TRANSACTION_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_DB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_TB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.USE_SQL;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.AsyncTaskCallBack;
import io.mycat.proxy.MycatSessionView;
import io.mycat.proxy.packet.MySQLPacketUtil;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import io.mycat.router.ResultRoute;
import io.mycat.router.routeResult.GlobalTableWriteResultRoute;
import io.mycat.router.routeResult.OneServerResultRoute;
import io.mycat.router.util.RouterUtil;
import io.mycat.sqlparser.util.BufferSQLContext;
import java.util.Collection;

/**
 * @author jamie12221
 * @date 2019-05-17 17:37
 **/
public interface QueryHandler {

  MycatRouter router();

  default void doQuery(byte[] sqlBytes, MycatSessionView mycat) {
    /**
     * 获取默认的schema
     */
    MycatSchema useSchema = mycat.getSchema();
    if (useSchema == null) {
      useSchema = router().getDefaultSchema();
    }
    String orgin = new String(sqlBytes);
    BufferSQLContext sqlContext = router().simpleParse(orgin);
    String sql = RouterUtil.removeSchema(orgin, useSchema.getSchemaName());
    byte sqlType = sqlContext.getSQLType();
    try {
      switch (sqlType) {
        case USE_SQL: {
          String schemaName = sqlContext.getSchemaName(0);
          useSchema(mycat, schemaName);
          break;
        }
        case SET_AUTOCOMMIT_SQL: {
          Boolean autocommit = sqlContext.isAutocommit();
          if (autocommit == null) {
            mycat.setLastMessage("set autocommit fail!");
            mycat.writeErrorEndPacket();
            return;
          } else {
            mycat.setAutoCommit(autocommit ? MySQLAutoCommit.ON : MySQLAutoCommit.OFF);
            mycat.writeOkEndPacket();
            return;
          }
        }
        case SET_TRANSACTION_SQL: {
          if (sqlContext.isAccessMode()) {
            mycat.setLastMessage("unsupport access mode");
            mycat.writeErrorEndPacket();
            return;
          }
          MySQLIsolation isolation = sqlContext.getIsolation();
          if (isolation == null) {
            mycat.setLastMessage("set transaction fail!");
            mycat.writeErrorEndPacket();
            return;
          }
          mycat.setIsolation(isolation);
          mycat.writeOkEndPacket();
          return;
        }
        case SHOW_DB_SQL: {
          MycatRouterConfig config = router().getConfig();
          showDb(mycat, config.getSchemaList());
          break;
        }
        case SHOW_TB_SQL: {
          String schemaName =
              sqlContext.getSchemaCount() == 1 ? sqlContext.getSchemaName(0)
                  : useSchema.getSchemaName();
          showTable(mycat, schemaName);
          break;
        }
        case DESCRIBE_SQL:
          mycat.setLastMessage("unsupport desc");
          mycat.writeErrorEndPacket();
          return;
        case SHOW_SQL:
          mycat.setLastMessage("unsupport show");
          mycat.writeErrorEndPacket();
          return;
        case SELECT_SQL: {
          if (sqlContext.isSimpleSelect()) {
            ResultRoute resultRoute = router().enterRoute(useSchema, sqlContext, sql);
            if (resultRoute != null) {
              switch (resultRoute.getType()) {
                case ONE_SERVER_RESULT_ROUTE:
                  OneServerResultRoute route = (OneServerResultRoute) resultRoute;
                  mycat
                      .proxyBackend(MySQLPacketUtil.generateComQuery(route.getSql()),
                          route.getDataNode(), true, null, false,
                          (session1, sender, success, result, attr) -> {
                            if (success) {
                              System.out.println("success full");
                            } else {
                              session1.writeErrorEndPacketBySyncInProcessError();
                            }
                          });
                  return;
              }
            }
            mycat.setLastMessage("unsupport sql");
            mycat.writeErrorEndPacket();
            return;//路由出错走默认节点
          }
        }

        default:
          if (sqlContext.getSQLType() != 0 & sqlContext.getTableCount() != 1) {
            mycat.setLastMessage("unsupport sql");
            mycat.writeErrorEndPacket();
            return;
          }
          ResultRoute resultRoute = router().enterRoute(useSchema, sqlContext, sql);
          if (resultRoute == null) {
            mycat.writeOkEndPacket();
            return;
          }
          switch (resultRoute.getType()) {
            case ONE_SERVER_RESULT_ROUTE: {
              OneServerResultRoute resultRoute1 = (OneServerResultRoute) resultRoute;
              mycat
                  .proxyBackend(MySQLPacketUtil.generateComQuery(resultRoute1.getSql()),
                      resultRoute1.getDataNode(), false, null, false,
                      (session1, sender, success, result, attr) -> {
                        if (success) {
                          System.out.println("success full");
                        } else {
                          session1.writeErrorEndPacket();
                        }
                      });
              break;
            }
            case GLOBAL_TABLE_WRITE_RESULT_ROUTE: {
              GlobalTableWriteResultRoute globalTableWriteResultRoute = (GlobalTableWriteResultRoute) resultRoute;
              String sql1 = globalTableWriteResultRoute.getSql();
              String master = globalTableWriteResultRoute.getMaster();
              Collection<String> dataNodes = globalTableWriteResultRoute.getDataNodes();
              mycat.proxyUpdateMultiBackends(MySQLPacketUtil.generateComQuery(sql1), master,
                  dataNodes, new AsyncTaskCallBack<MycatSessionView>() {
                    @Override
                    public void finished(MycatSessionView session, Object sender, boolean success,
                        Object result, Object attr) {
                      if (success) {
                        System.out.println("success full");
                      } else {
                        session.setLastMessage(result.toString());
                        session.writeErrorEndPacket();
                      }
                    }
                  });
              return;
            }
            default:
              mycat.setLastMessage("unsupport sql");
              mycat.writeErrorEndPacket();
          }

      }
    } catch (Exception e) {
      mycat.setLastMessage(e);
      mycat.writeErrorEndPacket();
    }
  }

  default void showDb(MycatSessionView mycat, Collection<MycatSchema> schemaList) {
    mycat.writeColumnCount(1);
    mycat.writeColumnDef("Dababase", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
    mycat.writeColumnEndPacket();
    for (MycatSchema schema : schemaList) {
      String schemaName = schema.getSchemaName();
      mycat.writeTextRowPacket(new byte[][]{schemaName.getBytes(mycat.charset())});
    }
    mycat.countDownResultSet();
    mycat.writeRowEndPacket(mycat.hasResultset(), mycat.hasCursor());
  }

  default void showTable(MycatSessionView mycat, String schemaName) {
    Collection<String> tableName = router().getConfig().getSchemaBySchemaName(schemaName)
                                       .getMycatTables().keySet();
    mycat.writeColumnCount(2);
    mycat.writeColumnDef("Tables in " + tableName, MySQLFieldsType.FIELD_TYPE_VAR_STRING);
    mycat.writeColumnDef("Table_type " + tableName, MySQLFieldsType.FIELD_TYPE_VAR_STRING);
    mycat.writeColumnEndPacket();
    MycatRouterConfig config = router().getConfig();
    MycatSchema schema = config.getSchemaBySchemaName(schemaName);
    byte[] basetable = mycat.encode("BASE TABLE");
    for (String name : schema.getMycatTables().keySet()) {
      mycat.writeTextRowPacket(new byte[][]{mycat.encode(name), basetable});
    }
    mycat.writeRowEndPacket(mycat.hasResultset(), mycat.hasCursor());
  }

  default void useSchema(MycatSessionView mycat, String schemaName) {
    MycatSchema schema = router().getConfig().getSchemaBySchemaName(schemaName);
    mycat.useSchema(schema);
    mycat.writeOkEndPacket();
  }
}

package io.mycat.command;

import static io.mycat.sqlparser.util.BufferSQLContext.DESCRIBE_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SELECT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_AUTOCOMMIT_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_CHARSET;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_CHARSET_RESULT;
import static io.mycat.sqlparser.util.BufferSQLContext.SET_TRANSACTION_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_DB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.SHOW_TB_SQL;
import static io.mycat.sqlparser.util.BufferSQLContext.USE_SQL;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.proxy.MySQLPacketUtil;
import io.mycat.proxy.MySQLTaskUtil;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import io.mycat.router.ResultRoute;
import io.mycat.router.routeResult.GlobalTableWriteResultRoute;
import io.mycat.router.routeResult.OneServerResultRoute;
import io.mycat.router.util.RouterUtil;
import io.mycat.security.MycatUser;
import io.mycat.sqlparser.util.BufferSQLContext;
import java.util.Collection;
import java.util.Collections;

/**
 * @author jamie12221
 * @date 2019-05-17 17:37
 **/
public interface QueryHandler {

  MycatRouter router();

  default void doQuery(byte[] sqlBytes, MycatSession mycat) {
    MycatRouterConfig routerConfig = ProxyRuntime.INSTANCE.getRouterConfig();

    /**
     * 获取默认的schema
     */
    MycatSchema useSchema = mycat.getSchema();
    if (useSchema == null) {
      useSchema = router().getDefaultSchema();
    }
    MycatUser user = mycat.getUser();
    String orgin = new String(sqlBytes);
    MycatMonitor.onOrginSQL(mycat, orgin);
    orgin = routerConfig.getSqlInterceptor().interceptSQL(orgin);
    BufferSQLContext sqlContext = router().simpleParse(orgin);
    String sql = RouterUtil.removeSchema(orgin, useSchema.getSchemaName());
    byte sqlType = sqlContext.getSQLType();

    if (!user.checkSQL(sqlType, sql, Collections.EMPTY_SET)) {
      mycat.setLastMessage("Because the security policy is not enforceable");
      mycat.writeErrorEndPacket();
      return;
    }
    if (mycat.isBindMySQLSession()) {
      MySQLTaskUtil.proxyBackend(mycat, MySQLPacketUtil.generateComQuery(sql),
          mycat.getMySQLSession().getDataNode().getName(), false, null, false
      );
      return;
    }
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
        case SET_CHARSET: {
          String charset = sqlContext.getCharset();
          mycat.setCharset(charset);
          mycat.writeOkEndPacket();
          return;
        }
        case SET_CHARSET_RESULT: {
          String charsetSetResult = sqlContext.getCharsetSetResult();
          mycat.setCharsetSetResult(charsetSetResult);//@todo but do no thing
          mycat.writeOkEndPacket();
          return;
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
//          mycat.setLastMessage("unsupport desc");
//          mycat.writeErrorEndPacket();
//          return;
        case SHOW_SQL:
          String defaultDataNode = useSchema.getDefaultDataNode();
          MySQLTaskUtil.proxyBackend(mycat, MySQLPacketUtil.generateComQuery(sql), defaultDataNode
              , false, null, false
          );
          return;
        case SELECT_SQL: {
          if (sqlContext.isSimpleSelect()) {
            ResultRoute resultRoute = router().enterRoute(useSchema, sqlContext, sql);
            if (resultRoute != null) {
              switch (resultRoute.getType()) {
                case ONE_SERVER_RESULT_ROUTE:
                  OneServerResultRoute route = (OneServerResultRoute) resultRoute;
                  MySQLTaskUtil
                      .proxyBackend(mycat, MySQLPacketUtil.generateComQuery(route.getSql()),
                          route.getDataNode(), true, null, false
                      );
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
              MySQLTaskUtil
                  .proxyBackend(mycat, MySQLPacketUtil.generateComQuery(resultRoute1.getSql()),
                      resultRoute1.getDataNode(), false, null, false
                  );
              break;
            }
            case GLOBAL_TABLE_WRITE_RESULT_ROUTE: {
              GlobalTableWriteResultRoute globalTableWriteResultRoute = (GlobalTableWriteResultRoute) resultRoute;
              String sql1 = globalTableWriteResultRoute.getSql();
              String master = globalTableWriteResultRoute.getMaster();
              Collection<String> dataNodes = globalTableWriteResultRoute.getDataNodes();
//              mycat.proxyUpdateMultiBackends(MySQLPacketUtil.generateComQuery(sql1), master,
//                  dataNodes, new AsyncTaskCallBack<MycatSessionView>() {
//                    @Override
//                    public void finished(MycatSessionView session, Object sender, boolean success,
//                        Object result, Object attr) {
//                      if (success) {
//                        System.out.println("success full");
//                      } else {
//                        session.setLastMessage(result.toString());
//                        session.writeErrorEndPacket();
//                      }
//                    }
//                  });
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

  default void showDb(MycatSession mycat, Collection<MycatSchema> schemaList) {
    mycat.writeColumnCount(1);
    mycat.writeColumnDef("Dababase", MySQLFieldsType.FIELD_TYPE_VAR_STRING);
    mycat.writeColumnEndPacket();
    for (MycatSchema schema : mycat.getUser().getSchemas().values()) {
      String schemaName = schema.getSchemaName();
      mycat.writeTextRowPacket(new byte[][]{schemaName.getBytes(mycat.charset())});
    }
    mycat.countDownResultSet();
    mycat.writeRowEndPacket(mycat.hasResultset(), mycat.hasCursor());
  }

  default void showTable(MycatSession mycat, String schemaName) {
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

  default void useSchema(MycatSession mycat, String schemaName) {
    MycatSchema schema = router().getConfig().getSchemaBySchemaName(schemaName);
    mycat.useSchema(schema);
    mycat.writeOkEndPacket();
  }
}

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
package io.mycat.command;

import io.mycat.beans.mycat.MycatSchema;
import io.mycat.command.loaddata.LoaddataContext;
import io.mycat.command.prepareStatement.PrepareStmtContext;
import io.mycat.config.schema.SchemaType;
import io.mycat.grid.BlockCommandHandler;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;
import io.mycat.router.ResultRoute;
import io.mycat.router.routeResult.OneServerResultRoute;
import io.mycat.sqlparser.util.BufferSQLContext;

/**
 * @author jamie12221 date 2019-05-13 02:47
 **/
public class MycatCommandHandler extends AbstractCommandHandler {

  private MycatRouter router;
  private MycatSession mycat;
  private PrepareStmtContext prepareContext;
  private final LoaddataContext loadDataContext = new LoaddataContext();
  private ProxyQueryHandler proxyQueryHandler;
  private BlockCommandHandler serverQueryHandler;

  @Override
  public void initRuntime(MycatSession mycat, ProxyRuntime runtime) {
    this.mycat = mycat;
    this.router = new MycatRouter((MycatRouterConfig) runtime.getDefContext().get("routeConfig"));
    this.prepareContext = new PrepareStmtContext(mycat);
    this.proxyQueryHandler = new ProxyQueryHandler(router, runtime);
    this.serverQueryHandler = new BlockCommandHandler(router, runtime);
  }

  @Override
  public void handleQuery(byte[] sqlBytes, MycatSession mycat) {
    MycatSchema schema = router.getSchemaBySchemaName(mycat.getSchema());
    if (schema == null) {
      schema = router.getDefaultSchema();
    }
    SchemaType schemaType = schema.getSchemaType();
    if (schemaType == SchemaType.SQL_PARSE_ROUTE) {
      serverQueryHandler.handleQuery(sqlBytes, mycat);
      return;
    }
    proxyQueryHandler.doQuery(schema, sqlBytes, mycat);
  }


  @Override
  public void handleContentOfFilename(byte[] sql, MycatSession seesion) {
    this.loadDataContext.append(sql);
  }

  @Override
  public void handleContentOfFilenameEmptyOk(MycatSession mycat) {
    this.loadDataContext.proxy(mycat);
  }

  @Override
  public void handlePrepareStatement(byte[] bytes, MycatSession mycat) {
    MycatSchema schema = router.getSchemaBySchemaName(mycat.getSchema());
    if (schema == null) {
      mycat.setLastMessage("not select schema");
      mycat.writeErrorEndPacket();
      return;
    }
    String sql = new String(bytes);
    BufferSQLContext bufferSQLContext = router.simpleParse(sql);
    ResultRoute resultRoute = router.enterRoute(schema, bufferSQLContext, sql);
    switch (resultRoute.getType()) {
      case ONE_SERVER_RESULT_ROUTE:
        OneServerResultRoute route = (OneServerResultRoute) resultRoute;
        LoadBalanceStrategy balance = mycat.getRuntime()
            .getLoadBalanceByBalanceName(resultRoute.getBalance());
        String dataNode = schema.getDefaultDataNode();
        mycat.switchDataNode(dataNode);
        prepareContext.newReadyPrepareStmt(sql, dataNode, route.isRunOnMaster(false), balance);
        return;
      default:
        mycat.setLastMessage(
            "MySQLProxyPrepareStatement only support in DB_IN_ONE_SERVER or DB_IN_MULTI_SERVER");
        mycat.writeErrorEndPacket();
    }
  }

  @Override
  public void handlePrepareStatementLongdata(long statementId, int paramId, byte[] data,
      MycatSession mycat) {
    MycatSchema schema = router.getSchemaBySchemaName(mycat.getSchema());
    if (schema == null) {
      mycat.setLastMessage("not select schema");
      mycat.writeErrorEndPacket();
      return;
    }

    if (schema.getSchemaType() == SchemaType.DB_IN_ONE_SERVER) {
      prepareContext.appendLongData(statementId, paramId, data);
    } else {
      mycat.setLastMessage(
          "MySQLProxyPrepareStatement only support in DB_IN_ONE_SERVER or DB_IN_MULTI_SERVER");
      mycat.writeErrorEndPacket();
    }
  }

  @Override
  public void handlePrepareStatementExecute(byte[] rawPayload, long statementId, byte flags,
      int numParams,
      byte[] rest,
      MycatSession mycat) {

    /////////////////route//////////////////

    //////////////////////////////////
    prepareContext.execute(statementId, flags, numParams, rest, mycat.getDataNode(), true, null);
  }


  @Override
  public void handlePrepareStatementClose(long statementId, MycatSession session) {
    prepareContext.close(statementId);
  }

  @Override
  public void handlePrepareStatementFetch(long statementId, long row, MycatSession mycat) {
    prepareContext.fetch(statementId, row);
  }

  @Override
  public void handlePrepareStatementReset(long statementId, MycatSession session) {
    prepareContext.reset(statementId);
  }

  @Override
  public int getNumParamsByStatementId(long statementId, MycatSession mycat) {
    return prepareContext.getNumOfParams(statementId);
  }
}

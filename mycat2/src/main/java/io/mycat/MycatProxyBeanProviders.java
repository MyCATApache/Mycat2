package io.mycat;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.command.CommandDispatcher;
import io.mycat.command.MycatCommandHandler;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.MySQLDataSourceEx;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import io.mycat.router.MycatRouter;
import io.mycat.router.MycatRouterConfig;

/**
 * @author jamie12221
 *  date 2019-05-22 22:12
 **/
public enum MycatProxyBeanProviders implements ProxyBeanProviders {
  INSTANCE;

  @Override
  public MySQLDatasource createDatasource(int index, DatasourceConfig datasourceConfig,
      MySQLReplica replica) {
    return new MySQLDataSourceEx(index, datasourceConfig, replica);
  }

  @Override
  public MySQLReplica createReplica(ReplicaConfig replicaConfig, int writeIndex) {
    return new MySQLReplica(replicaConfig, writeIndex, this) {
    };
  }

  @Override
  public MySQLDataNode createMySQLDataNode(DataNodeConfig config) {
    return new MySQLDataNode(config);
  }

  @Override
  public MycatCommandHandler createCommandDispatcher(MycatSession session) {
    MycatRouterConfig routerConfig = ProxyRuntime.INSTANCE.getRouterConfig();
    MycatRouter router = new MycatRouter(routerConfig);
    return new MycatCommandHandler(router,session);
  }





}

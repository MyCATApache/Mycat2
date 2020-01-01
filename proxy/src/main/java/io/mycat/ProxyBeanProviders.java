package io.mycat;

import io.mycat.command.CommandDispatcher;
import io.mycat.config.ClusterRootConfig;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.proxy.handler.backend.MySQLSynContext;
import io.mycat.proxy.session.MySQLClientSession;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;

import java.util.Map;
import java.util.Set;

/**
 * @author jamie12221 date 2019-05-22 21:36
 **/
public interface ProxyBeanProviders {

  void initRuntime( Map<String, Object> defContext) throws Exception;

  void beforeAcceptConnectionProcess(Map<String, Object> defContext) throws Exception;

  MySQLDatasource createDatasource(DatasourceRootConfig.DatasourceConfig  datasourceConfig);

  MySQLReplica createReplica(ClusterRootConfig replicaConfig,
                             Set<Integer> writeIndex);

  CommandDispatcher createCommandDispatcher(MycatSession session);

  MySQLSynContext createMySQLSynContext(MycatSession mycat);

  MySQLSynContext createMySQLSynContext(MySQLClientSession mysql);


  
}

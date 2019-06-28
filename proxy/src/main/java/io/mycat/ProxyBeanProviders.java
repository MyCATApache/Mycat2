package io.mycat;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.command.CommandDispatcher;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import java.util.Map;
import java.util.Set;

/**
 * @author jamie12221 date 2019-05-22 21:36
 **/
public interface ProxyBeanProviders {

  void initRuntime(ProxyRuntime runtime, Map<String, Object> defContext);


  MySQLDatasource createDatasource(ProxyRuntime runtime,int index, DatasourceConfig datasourceConfig,
      MySQLReplica replica);

  MySQLReplica createReplica(ProxyRuntime runtime,ReplicaConfig replicaConfig, Set<Integer> writeIndex);

  MySQLDataNode createMySQLDataNode(ProxyRuntime runtime,DataNodeConfig config);

  CommandDispatcher createCommandDispatcher(ProxyRuntime runtime,MycatSession session);
}

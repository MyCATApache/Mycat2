package io.mycat;

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.command.CommandDispatcher;
import io.mycat.command.CommandDispatcher.AbstractCommandHandler;
import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.proxy.session.MycatSession;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import java.util.List;
import java.util.Set;

/**
 * @author jamie12221 date 2019-05-22 21:36
 **/
public interface ProxyBeanProviders {

  MySQLDatasource createDatasource(int index, DatasourceConfig datasourceConfig,
      MySQLReplica replica);

  MySQLReplica createReplica(ReplicaConfig replicaConfig, Set<Integer> writeIndex);

  MySQLDataNode createMySQLDataNode(DataNodeConfig config);

  CommandDispatcher createCommandDispatcher(MycatSession session);

}

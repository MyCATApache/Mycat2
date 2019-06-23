package io.mycat.replica;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.replica.heartbeat.MysqlHeartBeatManager;

/**
 * @author jamie12221
 *  date 2019-05-14 19:36
 **/
public class MySQLDataSourceEx extends MySQLDatasource {
  final MysqlHeartBeatManager mysqlHeartBeatManager;

  public MySQLDataSourceEx(ProxyRuntime runtime,int index, DatasourceConfig datasourceConfig,
      MySQLReplica replica) {
    super(index, datasourceConfig, replica);
    mysqlHeartBeatManager = new MysqlHeartBeatManager(runtime,replica.getConfig(), this);
  }

  public void heartBeat() {
    mysqlHeartBeatManager.heartBeat();
  }

  @Override
  public boolean isAlive() {
    if(isMaster()) {
      return mysqlHeartBeatManager.getHeartBeatStatus().isAlive();
    } else {
      return mysqlHeartBeatManager.getHeartBeatStatus().isAlive()&& asSelectRead();
    }
  }

  @Override
  public boolean asSelectRead() {
    return mysqlHeartBeatManager.getHeartBeatStatus().isAlive()
        && mysqlHeartBeatManager.getHeartBeatStatus().isSlaveBehindMaster() == false
        && mysqlHeartBeatManager.getHeartBeatStatus().isDbSynStatusNormal();
  }
}

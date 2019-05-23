package io.mycat.replica;

import io.mycat.config.datasource.DatasourceConfig;
import io.mycat.replica.heartbeat.MysqlHeartBeatManager;

/**
 * @author jamie12221
 * @date 2019-05-14 19:36
 **/
public class MySQLDataSourceEx extends MySQLDatasource {
  final MysqlHeartBeatManager mysqlHeartBeatManager;

  public MySQLDataSourceEx(int index, DatasourceConfig datasourceConfig,
      MySQLReplica replica) {
    super(index, datasourceConfig, replica);
    mysqlHeartBeatManager = new MysqlHeartBeatManager(replica.getConfig(), this);
  }

  public void heartBeat() {
    mysqlHeartBeatManager.heartBeat();
  }

  @Override
  public boolean isAlive() {
    return mysqlHeartBeatManager.getHeartBeatStatus().isAlive();
  }

}

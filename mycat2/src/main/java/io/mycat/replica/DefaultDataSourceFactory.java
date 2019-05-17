package io.mycat.replica;

import io.mycat.config.datasource.DatasourceConfig;

/**
 * @author jamie12221
 * @date 2019-05-14 19:34
 **/
public class DefaultDataSourceFactory implements MySQLDataSourceFactory {

  @Override
  public MySQLDatasource get(int index, DatasourceConfig datasourceConfig, MySQLReplica replica) {

    return new MySQLDataSourceEx(index, datasourceConfig, replica);
  }
}

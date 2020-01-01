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
package io.mycat.replica;

import io.mycat.ProxyBeanProviders;
import io.mycat.config.ClusterRootConfig;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import java.util.List;

/**
 * 集群管理,该类不执行心跳,也不管理jdbc的mysqlsession,只做均衡负载 集群状态在集群相关辅助类实现,辅助类使用定时器分发到执行.辅助类只能更改此类的writeIndex属性,其他属性不是线程安全,初始化之后只读
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class MySQLReplica {

  protected static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(MySQLReplica.class);
  protected final ReplicaDataSourceSelector selector = null;

  /**
   * 初始化mycat集群管理
   *
   * @param replicaConfig the config of replica
   * @param dataSourceFactory a factory to create dataSource
   */
  public MySQLReplica(ClusterRootConfig.ClusterConfig replicaConfig,
      ProxyBeanProviders dataSourceFactory) {
//    assert replicaConfig != null;
//    List<DatasourceRootConfig.DatasourceConfig> mysqls = replicaConfig.getReplicas();
//    assert mysqls != null;
//    this.runtime = runtime;
//    this.config = replicaConfig;
//    this.selector = ReplicaSelectorRuntime.INSTCANE
//        .getDataSourceSelector(replicaConfig.getName());
//    List<DatasourceConfig> datasources = config.getDatasources();
//    if (datasources != null) {
//      for (int i = 0; i < datasources.size(); i++) {
//        MySQLDatasource mySQLDatasource = null;
//        DatasourceConfig datasourceConfig = datasources.get(i);
//        if (MycatConfigUtil.isMySQLType(datasourceConfig)) {
//          mySQLDatasource = dataSourceFactory.createDatasource(runtime, i, datasourceConfig, this);
//        }
//        mySQLDatasources.add(mySQLDatasource);
//      }
//    }
  }

}

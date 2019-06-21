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
package io.mycat.proxy;

import io.mycat.ProxyBeanProviders;
import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mysql.MySQLVariables;
import io.mycat.buffer.BufferPool;
import io.mycat.config.ConfigEnum;
import io.mycat.config.ConfigLoader;
import io.mycat.config.ConfigReceiverImpl;
import io.mycat.config.datasource.MasterIndexesRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import io.mycat.config.plug.PlugRootConfig;
import io.mycat.config.proxy.MysqlServerVariablesRootConfig;
import io.mycat.config.proxy.ProxyConfig;
import io.mycat.config.proxy.ProxyRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeType;
import io.mycat.config.schema.SchemaRootConfig;
import io.mycat.config.user.UserRootConfig;
import io.mycat.plug.loadBalance.LoadBalanceManager;
import io.mycat.plug.loadBalance.LoadBalanceStrategy;
import io.mycat.proxy.buffer.ProxyBufferPoolMonitor;
import io.mycat.proxy.callback.AsyncTaskCallBack;
import io.mycat.proxy.callback.AsyncTaskCallBackCounter;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOAcceptor;
import io.mycat.proxy.session.MycatSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import io.mycat.router.MycatRouterConfig;
import io.mycat.security.MycatSecurityConfig;
import io.mycat.util.CharsetUtil;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyRuntime extends ConfigReceiverImpl {

  public static final ProxyRuntime INSTANCE = new ProxyRuntime();
  private static final Logger LOGGER = LoggerFactory.getLogger(ProxyRuntime.class);
  private final AtomicInteger sessionIdCounter = new AtomicInteger(1);
  private final Map<String, MySQLReplica> replicaMap = new HashMap<>();
  private final List<MySQLDatasource> datasourceList = new ArrayList<>();
  private final Map<String, MycatDataNode> dataNodeMap = new HashMap<>();
  private LoadBalanceManager loadBalanceManager = new LoadBalanceManager();
  private MycatRouterConfig routerConfig;
  private MycatSecurityConfig securityManager;
  private MySQLVariables variables;
  private NIOAcceptor acceptor;
  private MycatReactorThread[] reactorThreads;


  public void reset() {
    assert acceptor == null;
    this.sessionIdCounter.set(1);
    this.replicaMap.clear();
    this.datasourceList.clear();
    this.dataNodeMap.clear();
    this.loadBalanceManager = new LoadBalanceManager();
    this.routerConfig = null;
    this.securityManager = null;
    this.variables = null;
    this.reactorThreads = null;
  }

  public static String getResourcesPath(Class clazz) {
    try {
      return Paths.get(
          Objects.requireNonNull(clazz.getProtectionDomain().getCodeSource().getLocation().toURI()))
          .toAbsolutePath()
          .toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void initMySQLVariables() {
    MysqlServerVariablesRootConfig config = getConfig(ConfigEnum.VARIABLES);
    Objects.requireNonNull(config.getVariables());
    variables = new MySQLVariables(config.getVariables());
  }

  /**
   * Getter for property 'variables'.
   *
   * @return Value for property 'variables'.
   */
  public MySQLVariables getVariables() {
    return variables;
  }

  public int getMaxAllowedPacket() {
    return variables.getMaxAllowedPacket();
  }

  public void initDataNode() {
    SchemaRootConfig schemaConfig =
        routerConfig.getConfig(ConfigEnum.SCHEMA);

    for (DataNodeConfig dataNodeConfig : schemaConfig.getDataNodes()) {
      DataNodeType dataNodeType =
          dataNodeConfig.getType() == null ? DataNodeType.MYSQL : dataNodeConfig.getType();
      if (dataNodeType == DataNodeType.MYSQL) {
        MySQLDataNode mySQLDataNode = new MySQLDataNode(dataNodeConfig);
        dataNodeMap.put(dataNodeConfig.getName(), mySQLDataNode);
        String replicaName = mySQLDataNode.getReplicaName();
        MySQLReplica mySQLReplica = replicaMap.get(replicaName);
        mySQLDataNode.setReplica(mySQLReplica);
      }
    }
  }


  public void initRepliac(ProxyBeanProviders factory, AsyncTaskCallBack future) {
    ReplicasRootConfig dsConfig = getConfig(ConfigEnum.DATASOURCE);
    MasterIndexesRootConfig replicaIndexConfig = getConfig(ConfigEnum.REPLICA_INDEX);
    Map<String, Integer> replicaIndexes = replicaIndexConfig.getMasterIndexes();
    List<ReplicaConfig> replicas = dsConfig.getReplicas();
    int size = replicas.size();
    AsyncTaskCallBackCounter counter = new AsyncTaskCallBackCounter(size, future);
    for (int i = 0; i < size; i++) {
      ReplicaConfig replicaConfig = replicas.get(i);
      Integer writeIndex = replicaIndexes.get(replicaConfig.getName());
      MySQLReplica replica = factory
          .createReplica(replicaConfig, writeIndex == null ? 0 : writeIndex);
      replicaMap.put(replica.getName(), replica);
      replica.init(counter);
      datasourceList.addAll(replica.getDatasourceList());
    }
  }

  private io.mycat.config.proxy.ProxyConfig getProxy() {
    ProxyRootConfig proxyRootConfig = getConfig(ConfigEnum.PROXY);
    return proxyRootConfig.getProxy();
  }

  public void loadProxyConfig(String root) throws IOException {
    ConfigLoader.INSTANCE.loadProxy(root, this);
  }

  public void loadMycatConfig(String root) throws IOException {
    ConfigLoader.INSTANCE.loadMycat(root, this);
  }


  public String getIP() {
    return getProxy().getIp();
  }

  public int getPort() {
    return getProxy().getPort();
  }

  public int getBufferPoolPageSize() {
    return getProxy().getBufferPoolPageSize();
  }

  public int getBufferPoolChunkSize() {
    return getProxy().getBufferPoolChunkSize();
  }

  public int getBufferPoolPageNumber() {
    return getProxy().getBufferPoolPageNumber();
  }


  public void exit() {
    Objects.requireNonNull(acceptor);
    Objects.requireNonNull(reactorThreads);
    try {
      acceptor.close();
    } catch (IOException e) {
      LOGGER.error("{}", e);
    }
    for (MycatReactorThread reactorThread : reactorThreads) {
      try {
        reactorThread.close();
      } catch (IOException e) {
        LOGGER.error("{}", e);
      }
    }

  }

  public void initReactor(ProxyBeanProviders commandHandlerFactory, AsyncTaskCallBack future) {
    Objects.requireNonNull(commandHandlerFactory);
    Objects.requireNonNull(future);
    ProxyConfig proxy = getProxy();
    int reactorNumber = proxy.getReactorNumber();
    MycatReactorThread[] mycatReactorThreads = new MycatReactorThread[reactorNumber];
    this.setMycatReactorThreads(mycatReactorThreads);
    try {
      for (int i = 0; i < mycatReactorThreads.length; i++) {
        BufferPool bufferPool = new ProxyBufferPoolMonitor(getBufferPoolPageSize(),
            getBufferPoolChunkSize(),
            getBufferPoolPageNumber());
        mycatReactorThreads[i] = new MycatReactorThread(bufferPool,
            new MycatSessionManager(commandHandlerFactory));
        mycatReactorThreads[i].start();
      }
      future.onFinished(null, null, null);
    } catch (Exception e) {
      future.onFinished(null, null, null);
    }
  }

  public void initAcceptor() throws IOException {
    if (acceptor == null || !acceptor.isAlive()) {
      NIOAcceptor acceptor = new NIOAcceptor(null);
      this.setAcceptor(acceptor);
      acceptor.start();
      acceptor.startServerChannel(getIP(), getPort());
    }
  }

  public NIOAcceptor getAcceptor() {
    return acceptor;
  }

  public void setAcceptor(NIOAcceptor acceptor) {
    this.acceptor = acceptor;
  }

  public void setMycatReactorThreads(MycatReactorThread[] reactorThreads) {
    this.reactorThreads = reactorThreads;
  }

  public int genSessionId() {
    return sessionIdCounter.getAndIncrement();
  }

  public MycatReactorThread[] getMycatReactorThreads() {
    return reactorThreads;
  }

  public <T extends MycatDataNode> T getDataNodeByName(String name) {
    return (T) dataNodeMap.get(name);
  }

  public MySQLReplica getMySQLReplicaByReplicaName(String name) {
    return replicaMap.get(name);
  }

  public String getCharsetById(int index) {
    return CharsetUtil.getCharset(index);
  }

  public <T extends MySQLDatasource> Collection<T> getMySQLDatasourceList() {
    return (Collection) datasourceList;
  }

  public <T extends MySQLReplica> Collection<T> getMySQLReplicaList() {
    return (Collection) replicaMap.values();
  }

  public MycatRouterConfig initRouterConfig(String root) {
    return this.routerConfig = new MycatRouterConfig(root);
  }


  public void initSecurityManager() {
    UserRootConfig userRootConfig = getConfig(ConfigEnum.USER);
    this.securityManager = new MycatSecurityConfig(userRootConfig, routerConfig);
  }


  public MycatSecurityConfig getSecurityManager() {
    return this.securityManager;
  }

  public MycatSchema getSchemaBySchemaName(String schemaName) {
    return this.routerConfig.getSchemaBySchemaName(schemaName);
  }

  public MycatRouterConfig getRouterConfig() {
    return routerConfig;
  }

  public void initCharset(String resourcesPath) {
    CharsetUtil.init(resourcesPath);
  }

  public void registerMonitor(MycatMonitorCallback callback) {
    MycatMonitor.setCallback(callback);
  }

  public void initPlug() {
    PlugRootConfig plugRootConfig = getConfig(ConfigEnum.PLUG);
    Objects.requireNonNull(plugRootConfig);
    loadBalanceManager.load(plugRootConfig);
  }

  public LoadBalanceStrategy getLoadBalanceByBalanceName(String name) {
    return loadBalanceManager.getLoadBalanceByBalanceName(name);
  }
}

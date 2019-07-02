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
import io.mycat.beans.mysql.MySQLVariables;
import io.mycat.buffer.BufferPool;
import io.mycat.config.ConfigEnum;
import io.mycat.config.ConfigReceiver;
import io.mycat.config.ConfigurableRoot;
import io.mycat.config.YamlUtil;
import io.mycat.config.datasource.MasterIndexesRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicasRootConfig;
import io.mycat.config.plug.PlugRootConfig;
import io.mycat.config.proxy.MysqlServerVariablesRootConfig;
import io.mycat.config.proxy.ProxyConfig;
import io.mycat.config.proxy.ProxyRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeRootConfig;
import io.mycat.config.schema.DataNodeType;
import io.mycat.config.user.UserRootConfig;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
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
import io.mycat.security.MycatSecurityConfig;
import io.mycat.util.CharsetUtil;
import io.mycat.util.StringUtil;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyRuntime {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(ProxyRuntime.class);
  private final AtomicInteger sessionIdCounter = new AtomicInteger(1);
  private final Map<String, MySQLReplica> replicaMap = new HashMap<>();
  private final List<MySQLDatasource> datasourceList = new ArrayList<>();
  private final Map<String, MycatDataNode> dataNodeMap = new HashMap<>();
  private LoadBalanceManager loadBalanceManager = new LoadBalanceManager();
  private MycatSecurityConfig securityManager;
  private MySQLVariables variables;
  private NIOAcceptor acceptor;
  private MycatReactorThread[] reactorThreads;
  private final ConfigReceiver config;
  private ProxyBeanProviders providers;
  private final Map<String,Object> defContext = new HashMap<>();

  public ProxyRuntime(ConfigReceiver configReceiver, ProxyBeanProviders providers) {
    this.config = configReceiver;
    this.providers = providers;
    this.initCharset(configReceiver.getResourcePath());
    this.initMySQLVariables();
    this.initPlug();
    this.initSecurityManager();
    this.initRepliac(this, providers);
    this.initDataNode(providers, configReceiver.getConfig(ConfigEnum.DATANODE));

    providers.initRuntime(this,defContext);
  }

  public void startReactor() throws IOException {
    initReactor(providers, this);
    initMinCon();
  }

  public void startAcceptor() throws IOException {
    initAcceptor();
  }


  public void reset() {
    acceptor = null;
    this.sessionIdCounter.set(1);
    this.replicaMap.clear();
    this.datasourceList.clear();
    this.dataNodeMap.clear();
    this.loadBalanceManager = new LoadBalanceManager();
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

  private void initMySQLVariables() {
    MysqlServerVariablesRootConfig config = getConfig(ConfigEnum.VARIABLES);
    Objects.requireNonNull(config.getVariables());
    variables = new MySQLVariables(config.getVariables());
  }

  public <T extends ConfigurableRoot> T getConfig(ConfigEnum configEnum) {
    ConfigurableRoot config = this.config.getConfig(configEnum);
    return (T) config;
  }

  public MySQLVariables getVariables() {
    return variables;
  }

  public int getMaxAllowedPacket() {
    return variables.getMaxAllowedPacket();
  }

  private void initDataNode(ProxyBeanProviders providers, DataNodeRootConfig dataNodeRootConfig) {
    for (DataNodeConfig dataNodeConfig : dataNodeRootConfig.getDataNodes()) {
      DataNodeType dataNodeType =
          dataNodeConfig.getType() == null ? DataNodeType.MYSQL : dataNodeConfig.getType();
      if (dataNodeType == DataNodeType.MYSQL) {
        MySQLDataNode mySQLDataNode = providers.createMySQLDataNode(this, dataNodeConfig);
        dataNodeMap.put(dataNodeConfig.getName(), mySQLDataNode);
        String replicaName = mySQLDataNode.getReplicaName();
        MySQLReplica mySQLReplica = replicaMap.get(replicaName);
        mySQLDataNode.setReplica(mySQLReplica);
      }
    }
  }


  private void initRepliac(ProxyRuntime runtime, ProxyBeanProviders factory) {
    ReplicasRootConfig dsConfig = getConfig(ConfigEnum.DATASOURCE);
    MasterIndexesRootConfig replicaIndexConfig = getConfig(ConfigEnum.REPLICA_INDEX);
    Map<String, String> replicaIndexes = replicaIndexConfig.getMasterIndexes();
    List<ReplicaConfig> replicas = dsConfig.getReplicas();
    int size = replicas.size();
    for (int i = 0; i < size; i++) {
      ReplicaConfig replicaConfig = replicas.get(i);
      String writeIndexText = replicaIndexes.get(replicaConfig.getName());
      Set<Integer> writeIndex;
      if (StringUtil.isEmpty(writeIndexText)) {
        writeIndex = Collections.singleton(0);
      } else {
        if (writeIndexText.contains(",")) {
          List<String> strings = Arrays.asList(writeIndexText.split(","));
          writeIndex = strings.stream().map(Integer::parseInt).collect(Collectors.toSet());
        } else {
          writeIndex = Collections.singleton(Integer.parseInt(writeIndexText));
        }
      }
      MySQLReplica replica = factory
          .createReplica(runtime, replicaConfig, writeIndex);
      replicaMap.put(replica.getName(), replica);
      datasourceList.addAll(replica.getDatasourceList());
    }
  }

  private io.mycat.config.proxy.ProxyConfig getProxy() {
    ProxyRootConfig proxyRootConfig = getConfig(ConfigEnum.PROXY);
    return proxyRootConfig.getProxy();
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


  public void exit(Exception message) {
    Objects.requireNonNull(acceptor);
    Objects.requireNonNull(reactorThreads);

    acceptor.close(message);
    for (MycatReactorThread reactorThread : reactorThreads) {
      reactorThread.close(message);
    }
    reset();
  }

  private void initReactor(ProxyBeanProviders providers, ProxyRuntime runtime) throws IOException {
    Objects.requireNonNull(providers);
    ProxyConfig proxy = getProxy();
    int reactorNumber = proxy.getReactorNumber();
    MycatReactorThread[] mycatReactorThreads = new MycatReactorThread[reactorNumber];
    this.setMycatReactorThreads(mycatReactorThreads);
    for (int i = 0; i < mycatReactorThreads.length; i++) {
      BufferPool bufferPool = new ProxyBufferPoolMonitor(getBufferPoolPageSize(),
          getBufferPoolChunkSize(),
          getBufferPoolPageNumber());
      mycatReactorThreads[i] = new MycatReactorThread(bufferPool,
          new MycatSessionManager(runtime, providers), runtime);
      mycatReactorThreads[i].start();
    }
  }

  private void initMinCon() {
    Objects.requireNonNull(reactorThreads);
    Objects.requireNonNull(datasourceList);
    for (MySQLDatasource datasource : datasourceList) {
      datasource.init(reactorThreads, new AsyncTaskCallBackCounter(datasourceList.size(),
          new AsyncTaskCallBack() {
            @Override
            public void onFinished(Object sender, Object result, Object attr) {

            }

            @Override
            public void onException(Exception e, Object sender, Object attr) {

            }
          }));
    }

  }

  private void initAcceptor() throws IOException {
    if (acceptor == null || !acceptor.isAlive()) {
      NIOAcceptor acceptor = new NIOAcceptor(null, this);
      this.setAcceptor(acceptor);
      acceptor.start();
      acceptor.startServerChannel(getIP(), getPort());
    }
  }

  private NIOAcceptor getAcceptor() {
    return acceptor;
  }

  private void setAcceptor(NIOAcceptor acceptor) {
    this.acceptor = acceptor;
  }

  private void setMycatReactorThreads(MycatReactorThread[] reactorThreads) {
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


  private void initSecurityManager() {
    UserRootConfig userRootConfig = getConfig(ConfigEnum.USER);
    this.securityManager = new MycatSecurityConfig(userRootConfig);
  }


  public MycatSecurityConfig getSecurityManager() {
    return this.securityManager;
  }


  private void initCharset(String resourcesPath) {
    CharsetUtil.init(resourcesPath);
  }

  public void registerMonitor(MycatMonitorCallback callback) {
    MycatMonitor.setCallback(callback);
  }

  private void initPlug() {
    PlugRootConfig plugRootConfig = getConfig(ConfigEnum.PLUG);
    Objects.requireNonNull(plugRootConfig);
    loadBalanceManager.load(plugRootConfig);
  }

  public LoadBalanceStrategy getLoadBalanceByBalanceName(String name) {
    return loadBalanceManager.getLoadBalanceByBalanceName(name);
  }


  public ConfigReceiver getConfig() {
    return config;
  }

  private static final Logger REPLICA_MASTER_INDEXES_LOGGER = LoggerFactory
      .getLogger("replicaIndexesLogger");


  public Map<String, Object> getDefContext() {
    return defContext;
  }

  /**
   * Getter for property 'providers'.
   *
   * @return Value for property 'providers'.
   */
  public ProxyBeanProviders getProviders() {
    return providers;
  }

  public void updateReplicaMasterIndexesConfig(final MySQLReplica replica,
      List<MySQLDatasource> writeDataSource) {

    synchronized (REPLICA_MASTER_INDEXES_LOGGER) {
      final MasterIndexesRootConfig config = getConfig(ConfigEnum.REPLICA_INDEX);
      Map<String, String> masterIndexes = new HashMap<>(config.getMasterIndexes());
      String name = replica.getName();
      String old = masterIndexes.get(name);
      String switchRes = writeDataSource.stream().map(i -> String.valueOf(i.getIndex()))
          .collect(Collectors.joining(","));
      if (old.equalsIgnoreCase(switchRes)) {
        return;
      }
      String backup = YamlUtil.dump(config);
      YamlUtil.dumpBackupToFile(config.getFilePath(), config.getVersion(), backup);
      masterIndexes.put(name, switchRes);
      config.setMasterIndexes(masterIndexes);
      config.setVersion(config.getVersion() + 1);
      String newContext = YamlUtil.dump(config);
      YamlUtil.dumpToFile(config.getFilePath(), newContext);
      REPLICA_MASTER_INDEXES_LOGGER.info("switchRes from:{}", old, switchRes);
    }

  }
}

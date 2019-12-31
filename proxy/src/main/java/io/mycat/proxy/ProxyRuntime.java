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


import io.mycat.ConfigRuntime;
import io.mycat.ProxyBeanProviders;
import io.mycat.beans.mysql.MySQLVariables;
import io.mycat.buffer.BufferPool;
import io.mycat.buffer.HeapBufferPool;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.ClusterRootConfig;
import io.mycat.config.ServerConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeRootConfig;
import io.mycat.config.schema.DataNodeType;
import io.mycat.config.SecurityConfig;
import io.mycat.ext.MySQLAPIRuntimeImpl;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.buffer.ProxyBufferPoolMonitor;
import io.mycat.proxy.callback.AsyncTaskCallBackCounter;
import io.mycat.proxy.callback.EmptyAsyncTaskCallBack;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.monitor.MycatMonitorCallback;
import io.mycat.proxy.reactor.MycatReactorThread;
import io.mycat.proxy.reactor.NIOAcceptor;
import io.mycat.proxy.session.MycatSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.security.MycatSecurityConfig;
import io.mycat.util.CharsetUtil;
import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;

public class ProxyRuntime {

  private static final MycatLogger LOGGER = MycatLoggerFactory.getLogger(ProxyRuntime.class);
  private final Map<String, MySQLReplica> replicaMap = new HashMap<>();
  private final Map<String, MySQLDatasource> datasourceMap = new HashMap<>();
  
  private final Map<String, Object> defContext = new HashMap<>();
  private final MySQLAPIRuntimeImpl mySQLAPIRuntime = new MySQLAPIRuntimeImpl();
  private volatile boolean gracefulShutdown = false;

  public ProxyRuntime(ConfigReceiver configReceiver)
      throws Exception {
    this.config = configReceiver;
    ProxyRootConfig config = this.config.getConfig(ConfigFile.PROXY);
    Objects.requireNonNull(config, "mycat.yaml was not found");
    String proxyBeanProviders = config.getProxy().getProxyBeanProviders();
    Objects.requireNonNull(proxyBeanProviders, "proxyBeanProviders was not found");
    this.providers = (ProxyBeanProviders) Class.forName(proxyBeanProviders).newInstance();
    this.initCharset(configReceiver.getResourcePath());
    this.initMySQLVariables();
    this.initSecurityManager();
    this.initRepliac(this, providers);
    this.initDataNode(providers, configReceiver.getConfig(ConfigFile.DATANODE));

    providers.initRuntime(this, defContext);
  }

  public void beforeAcceptConnectionProcess() throws Exception {
    providers.beforeAcceptConnectionProcess(this, defContext);
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
    this.datasourceMap.clear();
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


  public MySQLVariables getVariables() {
    return variables;
  }

  public int getMaxAllowedPacket() {
    return variables.getMaxAllowedPacket();
  }

  private void initDataNode(ProxyBeanProviders providers, DataNodeRootConfig dataNodeRootConfig) {
    ////////////////////////////////////check/////////////////////////////////////////////////
    Objects.requireNonNull(dataNodeRootConfig, "dataNode config not found");
    Objects.requireNonNull(dataNodeRootConfig.getDataNodes(), "datNode list can not be empty");
    for (DataNodeConfig dataNode : dataNodeRootConfig.getDataNodes()) {
      Objects.requireNonNull(dataNode.getName(), "dataNode name can not be empty");
      Objects.requireNonNull(dataNode.getDatabase(), "dataBase of dataNode can not be empty");
    }
    ////////////////////////////////////check/////////////////////////////////////////////////
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
    ReplicaSelectorRuntime.INSTCANE.load();
    ClusterRootConfig replicasRootConfig = ConfigRuntime.INSTCANE.getConfig(ConfigFile.DATASOURCE);

    for (ReplicaConfig config : replicasRootConfig.getReplicas()) {
      MySQLReplica replica = factory.createReplica(runtime, config,
          ConfigRuntime.INSTCANE.getReplicaIndexes(config.getName()));
      this.replicaMap.put(config.getName(), replica);
      for (MySQLDatasource datasource : replica.getDatasourceList()) {
        this.datasourceMap.put(datasource.getName(), datasource);
      }
    }
  }

  private ServerConfig getProxy() {
    ProxyRootConfig proxyRootConfig = getConfig(ConfigFile.PROXY);
    ////////////////////////////////////check/////////////////////////////////////////////////
    Objects.requireNonNull(proxyRootConfig, "proxy(mycat) config can not found");
    Objects.requireNonNull(proxyRootConfig.getProxy(), "proxy config can not be empty");
    ServerConfig proxy = proxyRootConfig.getProxy();
    Objects.requireNonNull(proxy.getCommandDispatcherClass(),
        "commandDispatcherClass can not be empty");
    Objects.requireNonNull(proxy.getIp(), "ip can not be empty");
    if (proxy.getReactorNumber() < 1) {
      LOGGER.warn("ReactorNumber:{}", proxy.getReactorNumber());
      proxy.setReactorNumber(1);
    }
    ////////////////////////////////////check/////////////////////////////////////////////////
    return proxyRootConfig.getProxy();
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
    ServerConfig proxy = getProxy();
    int reactorNumber = proxy.getReactorNumber();
    MycatReactorThread[] mycatReactorThreads = new MycatReactorThread[reactorNumber];
    this.setMycatReactorThreads(mycatReactorThreads);
    for (int i = 0; i < mycatReactorThreads.length; i++) {
      int bufferPoolPageSize = getBufferPoolPageSize();
      int bufferPoolChunkSize = getBufferPoolChunkSize();
      int bufferPoolPageNumber = getBufferPoolPageNumber();
      HeapBufferPool heapBufferPool = new HeapBufferPool();
      HashMap<String, String> config = new HashMap<>();
      config.put(HeapBufferPool.CHUNK_SIZE,String.valueOf(bufferPoolChunkSize));
      config.put(HeapBufferPool.PAGE_COUNT,String.valueOf(bufferPoolPageNumber));
      config.put(HeapBufferPool.PAGE_SIZE,String.valueOf(bufferPoolPageSize));
      heapBufferPool.init(config);
      BufferPool bufferPool = new ProxyBufferPoolMonitor(heapBufferPool);
      mycatReactorThreads[i] = new MycatReactorThread(bufferPool,
          new MycatSessionManager(runtime, providers), runtime);
      mycatReactorThreads[i].start();
    }
  }

  private void initMinCon() {
    Objects.requireNonNull(reactorThreads);
    Objects.requireNonNull(datasourceMap);
    for (MySQLDatasource datasource : datasourceMap.values()) {
      datasource.init(reactorThreads, new AsyncTaskCallBackCounter(datasourceMap.size(),
          EmptyAsyncTaskCallBack.INSTANCE));
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

  public CopyOnWriteArrayList<MycatReactorThread> getMycatReactorThreads() {
    return reactorThreads;
  }

  public MySQLReplica getMySQLReplicaByReplicaName(String name) {
    return replicaMap.get(name);
  }

  public MySQLDatasource getDataSourceByDataSourceName(String name) {
    return datasourceMap.get(name);
  }

  public String getCharsetById(int index) {
    return CharsetUtil.getCharset(index);
  }

  /**
   * not thread safe
   */
  public <T extends MySQLDatasource> Collection<T> getMySQLDatasourceList() {
    return (Collection) datasourceMap.values();
  }

  /**
   * not thread safe
   */
  public <T extends MySQLReplica> Collection<T> getMySQLReplicaList() {
    return (Collection) replicaMap.values();
  }


  private void initSecurityManager() {
    SecurityConfig userRootConfig = getConfig(ConfigFile.USER);
    Objects.requireNonNull(userRootConfig, "user config can not found");
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

  public Map<String, Object> getDefContext() {
    return defContext;
  }

  public ProxyBeanProviders getProviders() {
    return providers;
  }


  public MySQLAPIRuntimeImpl getMySQLAPIRuntime() {
    return mySQLAPIRuntime;
  }

  public void gracefulShutdown(){
    this.gracefulShutdown = true;
    ServerSocketChannel serverChannel = acceptor.getServerChannel();
    if (serverChannel!=null){
      try {
        serverChannel.close();
      } catch (IOException e) {
       LOGGER.error("",e);
      }
    }
  }

  public boolean isGracefulShutdown() {
    return gracefulShutdown;
  }
}

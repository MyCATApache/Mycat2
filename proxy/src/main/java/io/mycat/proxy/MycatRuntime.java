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

import io.mycat.MySQLDataNode;
import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.buffer.BufferPool;
import io.mycat.buffer.BufferPoolImpl;
import io.mycat.config.ConfigEnum;
import io.mycat.config.ConfigLoader;
import io.mycat.config.ConfigReceiverImpl;
import io.mycat.config.datasource.DatasourceRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicaIndexRootConfig;
import io.mycat.config.proxy.ProxyRootConfig;
import io.mycat.proxy.session.MycatSessionManager;
import io.mycat.replica.MySQLReplica;
import io.mycat.router.MycatRouterConfig;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MycatRuntime extends ConfigReceiverImpl {

  public static final MycatRuntime INSTANCE = new MycatRuntime();
  private static final Logger logger = LoggerFactory.getLogger(MycatRuntime.class);
  private final AtomicInteger sessionIdCounter = new AtomicInteger(0);
  private final MycatScheduler mycatScheduler = new MycatScheduler();
  private final MycatRouterConfig routerConfig = new MycatRouterConfig(getResourcesPath());
  private Map<String, MySQLReplica> replicaMap = new HashMap<>();

  public static String getResourcesPath() {
    try {
     return Paths.get(MycatRuntime.class.getClassLoader().getResource("").toURI()).toAbsolutePath()
          .toString();
    }catch (Exception e){
      throw new RuntimeException(e);
    }
  }
  public MycatRouterConfig getRouterConfig() {
    return routerConfig;
  }

  public MycatSchema getDefaultSchema() {
    return routerConfig.getDefaultSchema();
  }
  public MycatSchema getSchemaByName(String name) {
    return routerConfig.getSchemaBySchemaName(name);
  }
  public void initRepliac() {
    DatasourceRootConfig dsConfig = getConfig(ConfigEnum.DATASOURCE);
    ReplicaIndexRootConfig replicaIndexConfig = getConfig(ConfigEnum.REPLICA_INDEX);
    Map<String, Integer> replicaIndexes = replicaIndexConfig.getReplicaIndexes();
    for (ReplicaConfig replicaConfig : dsConfig.getReplicas()) {
      Integer writeIndex = replicaIndexes.get(replicaConfig.getName());
      MySQLReplica replica = new MySQLReplica(replicaConfig, writeIndex == null ? 0 : writeIndex);
      replicaMap.put(replica.getName(), replica);
      replica.init();
    }
    if (routerConfig != null) {
      for (MycatDataNode dataNode : routerConfig.getDataNodes()) {
        if (dataNode instanceof MySQLDataNode) {
          MySQLDataNode mySQLDataNode = (MySQLDataNode) dataNode;
          MySQLReplica mySQLReplica = replicaMap.get(mySQLDataNode.getReplicaName());
          mySQLDataNode.setReplica(mySQLReplica);
        }
      }

    }
  }

  private io.mycat.config.proxy.ProxyConfig getProxy() {
    ProxyRootConfig proxyRootConfig = getConfig(ConfigEnum.PROXY);
    return proxyRootConfig.getProxy();
  }

  public void loadProxy() throws IOException {
    ConfigLoader.INSTANCE.loadProxy(this);
  }

  public void loadMycat() throws IOException {
    ConfigLoader.INSTANCE.loadMycat(this);
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

  public MycatScheduler getMycatScheduler() {
    return mycatScheduler;
  }

  private NIOAcceptor acceptor;
  private MycatReactorThread[] reactorThreads;

  public void initReactor() throws IOException {
    int cpus = Runtime.getRuntime().availableProcessors();
    //MycatReactorThread[] mycatReactorThreads = new MycatReactorThread[Math.max(1,cpus-2)];
    MycatReactorThread[] mycatReactorThreads = new MycatReactorThread[1];
    this.setMycatReactorThreads(mycatReactorThreads);
    for (int i = 0; i < mycatReactorThreads.length; i++) {
      BufferPool bufferPool = new BufferPoolImpl(getBufferPoolPageSize(), getBufferPoolChunkSize(),
          getBufferPoolPageNumber());
      mycatReactorThreads[i] = new MycatReactorThread(bufferPool,
          new MycatSessionManager());
      mycatReactorThreads[i].start();
    }
  }

//  public void initHeartbeat() {
//    this.getMycatScheduler().scheduleAtFixedRate(() -> {
//      for (MySQLReplica replica : this.replicaMap.values()) {
//        replica.doHeartbeat();
//      }
//    }, Integer.MAX_VALUE, TimeUnit.DAYS);
//  }

  public void initAcceptor() throws IOException {
    NIOAcceptor acceptor = new NIOAcceptor(null);
    this.setAcceptor(acceptor);
    acceptor.start();
    acceptor.startServerChannel(getIP(), getPort());
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
    return sessionIdCounter.incrementAndGet();
  }

  public MycatReactorThread[] getMycatReactorThreads() {
    return reactorThreads;
  }

  public <T extends MycatDataNode> T getDataNodeByName(String name) {
    return (T)routerConfig.getDataNodeByName(name);
  }

  public MySQLReplica getMySQLReplicaByReplicaName(String name) {
    return replicaMap.get(name);
  }

  public void setCommandHandler() {

  }
}

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

import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.buffer.BufferPool;
import io.mycat.buffer.BufferPoolImpl;
import io.mycat.config.ConfigEnum;
import io.mycat.config.ConfigLoader;
import io.mycat.config.ConfigReceiverImpl;
import io.mycat.config.datasource.DatasourceRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicaIndexRootConfig;
import io.mycat.config.proxy.ProxyConfig;
import io.mycat.config.proxy.ProxyRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeType;
import io.mycat.config.schema.SchemaRootConfig;
import io.mycat.proxy.handler.CommandHandler;
import io.mycat.proxy.session.MycatSessionManager;
import io.mycat.replica.MySQLDatasource;
import io.mycat.replica.MySQLReplica;
import io.mycat.replica.MySQLReplicaFactory;
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
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyRuntime extends ConfigReceiverImpl {

  public static final ProxyRuntime INSTANCE = new ProxyRuntime();
  private static final Logger logger = LoggerFactory.getLogger(ProxyRuntime.class);
  private final AtomicInteger sessionIdCounter = new AtomicInteger(0);
  private final Map<String, MySQLReplica> replicaMap = new HashMap<>();
  private final List<MySQLDatasource> datasourceList = new ArrayList<>();
  private final Map<String, MycatDataNode> dataNodeMap = new HashMap<>();

  public static String getResourcesPath() {
    try {
      return Paths.get(
          Objects.requireNonNull(ProxyRuntime.class.getClassLoader().getResource("")).toURI())
                 .toAbsolutePath()
                 .toString();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  public int getMaxAllowedPacket() {
    return getProxy().getMaxAllowedPacket();
  }

  public void initDataNode() {
    SchemaRootConfig schemaConfig =
        getConfig(ConfigEnum.SCHEMA);

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


  public void initRepliac(MySQLReplicaFactory factory) {
    DatasourceRootConfig dsConfig = getConfig(ConfigEnum.DATASOURCE);
    ReplicaIndexRootConfig replicaIndexConfig = getConfig(ConfigEnum.REPLICA_INDEX);
    Map<String, Integer> replicaIndexes = replicaIndexConfig.getReplicaIndexes();
    for (ReplicaConfig replicaConfig : dsConfig.getReplicas()) {
      Integer writeIndex = replicaIndexes.get(replicaConfig.getName());
      MySQLReplica replica = factory.get(replicaConfig, writeIndex == null ? 0 : writeIndex);
      replicaMap.put(replica.getName(), replica);
      replica.init();
      datasourceList.addAll(replica.getDatasourceList());
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

  private NIOAcceptor acceptor;
  private MycatReactorThread[] reactorThreads;

  public void initReactor(Supplier<CommandHandler> commandHandlerFactory) throws IOException {
    ProxyConfig proxy = getProxy();
    int reactorNumber = proxy.getReactorNumber();
    MycatReactorThread[] mycatReactorThreads = new MycatReactorThread[reactorNumber];
    this.setMycatReactorThreads(mycatReactorThreads);
    for (int i = 0; i < mycatReactorThreads.length; i++) {
      BufferPool bufferPool = new BufferPoolImpl(getBufferPoolPageSize(), getBufferPoolChunkSize(),
          getBufferPoolPageNumber());
      mycatReactorThreads[i] = new MycatReactorThread(bufferPool,
          new MycatSessionManager(commandHandlerFactory));
      mycatReactorThreads[i].start();
    }
  }

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
    return (T) dataNodeMap.get(name);
  }

  public MySQLReplica getMySQLReplicaByReplicaName(String name) {
    return replicaMap.get(name);
  }

  public String getCharsetById(int index) {
    return CharsetUtil.getCharset(index);
  }

  public <T extends MySQLDatasource> Collection<T> getMySQLDatasourceList() {
    return (List) datasourceList;
  }
}

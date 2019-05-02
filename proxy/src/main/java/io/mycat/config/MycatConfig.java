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
package io.mycat.config;

import io.mycat.beans.DataNode;
import io.mycat.beans.Schema;
import io.mycat.beans.TableDef;
import io.mycat.config.datasource.DatasourceRootConfig;
import io.mycat.config.datasource.ReplicaConfig;
import io.mycat.config.datasource.ReplicaIndexRootConfig;
import io.mycat.config.proxy.ProxyConfig;
import io.mycat.config.proxy.ProxyRootConfig;
import io.mycat.config.route.DynamicAnnotationRootConfig;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.SchemaConfig;
import io.mycat.config.schema.SchemaRootConfig;
import io.mycat.replica.Replica;
import io.mycat.router.dynamicAnnotation.DynamicAnnotationMatcher;
import io.mycat.util.SplitUtil;
import java.io.IOException;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * chen junwen
 */
public class MycatConfig implements ConfigReceiver {

  // 当前节点所用的配置文件的版本
  private Map<ConfigEnum, Integer> configVersionMap = new EnumMap<>(ConfigEnum.class);
  private Map<ConfigEnum, Configurable> configMap = new EnumMap<>(ConfigEnum.class);
  private Map<ConfigEnum, Long> configUpdateTimeMap = new EnumMap<>(ConfigEnum.class);

  /**
   * 系统中所有MySQLRepBean的Map
   */
  private Map<String, Replica> replicaMap = new HashMap<>();
  /**
   * 系统中所有DataNode的Map
   */
  private Map<String, DataNode> dataNodeMap = new HashMap<>();

  /**
   * 系统中所有SchemaBean的Map
   */
  private Map<String, Schema> schemaMap = new HashMap<>();

  /**
   * 系统中所有TableDefBean的Map
   */
  private Map<String, TableDef> mycatTableMap = new HashMap<>();


  private Schema defaultSchema;

  public Map<String, Replica> getReplicaMap() {
    return replicaMap;
  }

  public Map<String, DataNode> getDataNodeMap() {
    return dataNodeMap;
  }

  public Map<String, Schema> getSchemaMap() {
    return schemaMap;
  }

  public Map<String, TableDef> getMycatTableMap() {
    return mycatTableMap;
  }

  public Schema getDefaultSchema() {
    return defaultSchema;
  }

  public DataNode getDataNodeByName(String name) {
    return dataNodeMap.get(name);
  }

  public Schema getSchemaByName(String name) {
    return schemaMap.get(name);
  }

  public void initRepliac() {
    DatasourceRootConfig dsConfig = getConfig(ConfigEnum.DATASOURCE);
    ReplicaIndexRootConfig replicaIndexConfig = getConfig(ConfigEnum.REPLICA_INDEX);
    Map<String, Integer> replicaIndexes = replicaIndexConfig.getReplicaIndexes();
    for (ReplicaConfig replicaConfig : dsConfig.getReplicas()) {
      Integer writeIndex = replicaIndexes.get(replicaConfig.getName());
      Replica replica = new Replica(replicaConfig, writeIndex == null ? 0 : writeIndex);
      replicaMap.put(replica.getName(), replica);
      replica.init();
    }
  }

  public void initSchema() {
    SchemaRootConfig schemaConfig = getConfig(ConfigEnum.SCHEMA);
    for (DataNodeConfig dataNodeConfig : schemaConfig.getDataNodes()) {
      Map<String, Replica> replicaMap = getReplicaMap();
      Replica replica = replicaMap.get(dataNodeConfig.getReplica());
      DataNode dataNode = new DataNode(dataNodeConfig.getName(), dataNodeConfig.getDatabase(),
          replica);
      dataNodeMap.put(dataNodeConfig.getName(), dataNode);
    }
    List<SchemaConfig> schemaConfigs = schemaConfig.getSchemas();
    Schema schema = null;
    for (int i = 0; i < schemaConfigs.size(); i++) {
      SchemaConfig config = schemaConfigs.get(i);
      schema = new Schema(config);
      schemaMap.put(config.getName(), schema);
      config.getTables().forEach(table -> {
        String[] theDataNodes = SplitUtil.split(table.getDataNode(), ',', '$', '-');
        if (theDataNodes.length <= 0) {
          throw new IllegalArgumentException("invalid table dataNodes: " + table.getDataNode());
        }
        for (String dn : theDataNodes) {
          table.getDataNodes().add(dn);
        }
        TableDef tableDef = new TableDef(table);
        mycatTableMap.put(table.getName(), tableDef);
      });
    }
    if (defaultSchema == null) {
      defaultSchema = schema;
    }
  }

  private ProxyConfig getProxy() {
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

  @Override
  public int getConfigVersion(ConfigEnum configEnum) {
    Integer oldVersion = configVersionMap.get(configEnum);
    return oldVersion == null ? GlobalConfig.INIT_VERSION : oldVersion;
  }

  @Override
  public void putConfig(ConfigEnum configEnum, Configurable config, int version) {
    configMap.put(configEnum, config);
    configVersionMap.put(configEnum, version);
    configUpdateTimeMap.put(configEnum, System.currentTimeMillis());
  }

  @Override
  public void setConfigVersion(ConfigEnum configEnum, int version) {
    configVersionMap.put(configEnum, version);
    configUpdateTimeMap.put(configEnum, System.currentTimeMillis());
  }

  @Override
  public <T extends Configurable> T getConfig(ConfigEnum configEnum) {
    return (T) configMap.get(configEnum);
  }

  public void initDynamicAnnotation() {
    DynamicAnnotationRootConfig dynamicAnnotationRootConfig =(DynamicAnnotationRootConfig) configMap.get(ConfigEnum.DYNAMIC_ANNOTATION);
    DynamicAnnotationMatcher dynamicAnnotationMatcher = new DynamicAnnotationMatcher(dynamicAnnotationRootConfig.getDynamicAnnotations());
    Map<String,String> matcher = dynamicAnnotationMatcher.match("id = 1 between 2 and 3");
//    while (matcher.find()){
//      System.out.printf("Group zero, start= %s, end= %s, match= '%s'%n",
//          matcher.start(), matcher.end(), matcher.group());
//      for (int i = 1; i <= matcher.groupCount(); i++) {
//        System.out.printf("Group number: %s, start: %s, end: %s, match= '%s'%n%n",
//            i, matcher.start(i), matcher.end(i), matcher.group(i));
//      }
//    }


  }
}

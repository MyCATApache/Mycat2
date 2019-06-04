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
package io.mycat.router;

import io.mycat.MycatExpection;
import io.mycat.beans.mycat.DefaultTable;
import io.mycat.beans.mycat.ERTable;
import io.mycat.beans.mycat.GlobalTable;
import io.mycat.beans.mycat.MySQLDataNode;
import io.mycat.beans.mycat.MycatDataNode;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mycat.MycatTable;
import io.mycat.beans.mycat.MycatTableRule;
import io.mycat.beans.mycat.ShardingDatabseTableTable;
import io.mycat.beans.mycat.ShardingDbTable;
import io.mycat.beans.mycat.ShardingTableTable;
import io.mycat.config.ConfigEnum;
import io.mycat.config.ConfigLoader;
import io.mycat.config.ConfigReceiverImpl;
import io.mycat.config.GlobalConfig;
import io.mycat.config.YamlUtil;
import io.mycat.config.route.AnnotationType;
import io.mycat.config.route.DynamicAnnotationConfig;
import io.mycat.config.route.DynamicAnnotationRootConfig;
import io.mycat.config.route.ShardingFuntion;
import io.mycat.config.route.ShardingRule;
import io.mycat.config.route.ShardingRuleRootConfig;
import io.mycat.config.route.SharingFuntionRootConfig;
import io.mycat.config.route.SharingTableRule;
import io.mycat.config.route.SubShardingFuntion;
import io.mycat.config.schema.DataNodeConfig;
import io.mycat.config.schema.DataNodeType;
import io.mycat.config.schema.SchemaConfig;
import io.mycat.config.schema.SchemaRootConfig;
import io.mycat.config.schema.SchemaType;
import io.mycat.config.schema.TableDefConfig;
import io.mycat.router.dynamicAnnotation.DynamicAnnotationMatcherImpl;
import io.mycat.router.routeStrategy.AnnotationRouteStrategy;
import io.mycat.router.routeStrategy.DbInMutilServerRouteStrategy;
import io.mycat.router.routeStrategy.DbInOneServerRouteStrategy;
import io.mycat.router.routeStrategy.SqlParseRouteRouteStrategy;
import io.mycat.util.SplitUtil;
import io.mycat.util.StringUtil;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * @author jamie12221
 *  date 2019-05-03 00:29
 **/
public class MycatRouterConfig extends ConfigReceiverImpl {

  private final Map<String, Supplier<RuleAlgorithm>> functions = new HashMap<>();
  private final Map<String, DynamicAnnotationConfig> dynamicAnnotations = new HashMap<>();
  private final Map<String, MycatTableRule> tableRules = new HashMap<>();
  private final Map<String, MycatSchema> schemas = new HashMap<>();
  private final Map<String, MycatDataNode> dataNodes = new HashMap<>();
  private final Map<String, List<MycatDataNode>> replicaNameToDataNodes = new HashMap<>();
  private final MycatSchema defaultSchema;
  private SQLInterceptor sqlInterceptor = (s) -> s;

  public Collection<MycatDataNode> getDataNodes() {
    return dataNodes.values();
  }

  public MycatTableRule getTableRuleByTableName(String name) {
    return tableRules.get(name);
  }

  public void putDynamicAnnotation(String name, DynamicAnnotationConfig dynamicAnnotation) {
    dynamicAnnotations.put(name, dynamicAnnotation);
  }

  public DynamicAnnotationMatcherImpl getDynamicAnnotationMatcher(List<String> names) {
    List<DynamicAnnotationConfig> list = new ArrayList<>();
    for (String name : names) {
      list.add(dynamicAnnotations.get(name));
    }
    return new DynamicAnnotationMatcherImpl(list);
  }

  /**/
  private static void init() throws CloneNotSupportedException {
    SchemaRootConfig schemaRootConfig = new SchemaRootConfig();

    SchemaConfig sc = new SchemaConfig();
    schemaRootConfig.setSchemas(Arrays.asList(sc));
    sc.setDefaultDataNode("dafault");
    sc.setName("schemaName");
    sc.setSchemaType(SchemaType.DB_IN_ONE_SERVER);
    sc.setSqlMaxLimit("100");

    TableDefConfig table = new TableDefConfig();

    sc.setTables(Arrays.asList(table));

    table.setTableRule("rule1");
    table.setDataNodes("dn1,dn2");

    ShardingRuleRootConfig src = new ShardingRuleRootConfig();

    SharingTableRule sharingTableRule = new SharingTableRule();
    src.setTableRules(Collections.singletonList(sharingTableRule));

    ShardingRule s1 = new ShardingRule();
    ShardingRule s2 = new ShardingRule();

    sharingTableRule.setRules(Arrays.asList(s1, s2));

    sharingTableRule.setFuntion("mpartitionByLong");

    s1.setColumn("id1");
    s2.setColumn("id2");

    s1.setEqualAnnotations(Arrays.asList("(?:id = )(?<id1>([0-9]))"));
    s2.setEqualAnnotations(Arrays.asList("(?:id = )(?<id2>([0-9]))"));
    s1.setEqualKey("id1");
    s2.setEqualKey("id2");

    s1.setRangeAnnotations(
        Arrays.asList("(?<between>((?:between )(?<id1s>[0-9])(?: and )(?<id1e>[0-9])))"));
    s2.setRangeAnnotations(
        Arrays.asList("(?<between>((?:between )(?<id2s>[0-9])(?: and )(?<id2e>[0-9])))"));

    s1.setRangeStartKey("id1s");
    s1.setRangeEndKey("id1e");

    s2.setRangeStartKey("id2s");
    s2.setRangeEndKey("id2e");

    SharingFuntionRootConfig sfrc = new SharingFuntionRootConfig();
    ShardingFuntion shardingFuntion = new ShardingFuntion();
    sfrc.setFuntions(Arrays.asList(shardingFuntion));
    shardingFuntion.setName("partitionByLong");
    shardingFuntion.setClazz("io.mycat.router.function.PartitionByLong");
    Map<String, String> properties = new HashMap<>();
    shardingFuntion.setProperties(properties);
    properties.put("partitionCount", "8");
    properties.put("partitionLength", "128");

    SubShardingFuntion subShardingFuntion = new SubShardingFuntion();
    subShardingFuntion.setClazz("io.mycat.router.function.PartitionByLong");
    Map<String, String> subProperties = new HashMap<>();
    subShardingFuntion.setProperties(Arrays.asList(subProperties));
    subProperties.put("partitionCount", "8");
    subProperties.put("partitionLength", "128");
    shardingFuntion.setSubFuntion(subShardingFuntion);

    String dump = YamlUtil.dump(src);
  }

  public static List<DynamicAnnotationConfig> getDynamicAnnotationConfigList(List<String> patterns,
      AnnotationType type) {
    List<DynamicAnnotationConfig> list = new ArrayList<>();
    for (String pattern : patterns) {
      DynamicAnnotationConfig dynamicAnnotationConfig = new DynamicAnnotationConfig();
      dynamicAnnotationConfig.setType(type);
      dynamicAnnotationConfig.setPattern(pattern);
      list.add(dynamicAnnotationConfig);
    }
    return list;
  }

  public void putRuleAlgorithm(ShardingFuntion funtion) {

    functions.put(funtion.getName(), () -> {
      try {
        String name = funtion.getName();
        RuleAlgorithm rootFunction = getRuleAlgorithm(funtion);
        ShardingFuntion rootConfig = funtion;
        SubShardingFuntion subFuntionConfig = rootConfig.getSubFuntion();
        if (subFuntionConfig != null) {
          rootFunction.setSubRuleAlgorithm(getSubRuleAlgorithmList(
              rootFunction.getPartitionNum(),
              name,
              subFuntionConfig));
        }
        return rootFunction;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private RuleAlgorithm getRuleAlgorithm(ShardingFuntion funtion)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    Map<String, String> properties = funtion.getProperties();
    properties = (properties == null) ? Collections.emptyMap() : properties;
    funtion.setProperties(properties);
    RuleAlgorithm rootFunction = createFunction(funtion.getName(), funtion.getClazz());
    rootFunction.init(funtion.getProperties());
    return rootFunction;
  }

  private List<RuleAlgorithm> getSubRuleAlgorithmList(int partitionNum, String parent,
      SubShardingFuntion funtion)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    List<Map<String, String>> properties = funtion.getProperties();
    List<RuleAlgorithm> ruleAlgorithms = new ArrayList<>();
    if (properties == null) {
      properties = Collections.EMPTY_LIST;
    }
    for (int i = 0; i < partitionNum; i++) {
      RuleAlgorithm function = createFunction(parent + funtion.toString(), funtion.getClazz());
      if (properties.size() == 1) {
        function.init(properties.get(0));
      } else if (properties.isEmpty()) {
        function.init(Collections.EMPTY_MAP);
      } else {
        function.init(properties.get(i));
      }
      ruleAlgorithms.add(function);
      if (funtion.getSubFuntion() != null) {
        function.setSubRuleAlgorithm(
            getSubRuleAlgorithmList(function.getPartitionNum(), parent, funtion.getSubFuntion()));
      }
    }
    return ruleAlgorithms;
  }

  public RuleAlgorithm getRuleAlgorithm(String name) {
    return functions.get(name).get();
  }


  public MycatRouterConfig(String root) {
    //init();
    ConfigLoader.INSTANCE
        .loadConfig(root, ConfigEnum.SCHEMA, GlobalConfig.INIT_VERSION, this);
    ConfigLoader.INSTANCE.loadConfig(root, ConfigEnum.RULE, GlobalConfig.INIT_VERSION, this);
    ConfigLoader.INSTANCE
        .loadConfig(root, ConfigEnum.FUNCTIONS, GlobalConfig.INIT_VERSION, this);
    ConfigLoader.INSTANCE
        .loadConfig(root, ConfigEnum.DYNAMIC_ANNOTATION, GlobalConfig.INIT_VERSION, this);

    initFunctions();
    initAnnotations();
    initTableRule();
    iniSchema();
    this.defaultSchema = initDefaultSchema();
    initDataNode();

  }

  public MycatSchema getDefaultSchema() {
    return defaultSchema;
  }

  private MycatSchema initDefaultSchema() {
    SchemaRootConfig schemaConfigs = this.getConfig(ConfigEnum.SCHEMA);
    String defaultSchemaName = schemaConfigs.getDefaultSchemaName();
    if (defaultSchemaName == null || "".equals(defaultSchemaName)) {
      defaultSchemaName = schemaConfigs.getSchemas().get(0).getName();
    }
    return schemas.get(defaultSchemaName);
  }

  private void initAnnotations() {
    MycatRouterConfig mycatRouter = this;
    DynamicAnnotationRootConfig config = mycatRouter
                                             .getConfig(ConfigEnum.DYNAMIC_ANNOTATION);
    if (config != null) {
      List<DynamicAnnotationConfig> annotations = config.getDynamicAnnotations();
      for (DynamicAnnotationConfig a : annotations) {
        mycatRouter.putDynamicAnnotation(a.getName(), a);
      }
    }
  }

  private void initDataNode() {
    MycatRouterConfig mycatRouter = this;
    SchemaRootConfig schemaConfig = mycatRouter
                                        .getConfig(ConfigEnum.SCHEMA);

    for (DataNodeConfig dataNodeConfig : schemaConfig.getDataNodes()) {
      DataNodeType dataNodeType =
          dataNodeConfig.getType() == null ? DataNodeType.MYSQL : dataNodeConfig.getType();
      switch (dataNodeType) {
        case MYSQL:
          MySQLDataNode mySQLDataNode = new MySQLDataNode(dataNodeConfig);
          dataNodes.put(dataNodeConfig.getName(), mySQLDataNode);
          replicaNameToDataNodes.compute(mySQLDataNode.getReplicaName(),
              (s, mycatDataNodes) -> {
                if (mycatDataNodes == null) {
                  mycatDataNodes = new ArrayList<>();
                }
                mycatDataNodes.add(mySQLDataNode);
                return mycatDataNodes;
              });
          break;
      }
    }
  }

  private void initFunctions() {
    MycatRouterConfig mycatRouter = this;
    SharingFuntionRootConfig funtions = mycatRouter.getConfig(ConfigEnum.FUNCTIONS);
    if (funtions != null) {
      for (ShardingFuntion funtion : funtions.getFuntions()) {
        mycatRouter.putRuleAlgorithm(funtion);
      }
    }
  }

//
//    String sql = "select * from travelrecord where  id = 1 and id2 between 2 and 3;";
//
//    DynamicAnnotationResultImpl result = matcher.match(sql);
//
//    List<MycatTable> tables = new ArrayList<>();
//    RouteContext sqlContext = new RouteContext();
//    sqlContext.setResult(result);
//    rootRouteNode.enterRoute(result, algorithm, sqlContext);

  public DynamicAnnotationMatcherImpl getDynamicAnnotationMatcherAndResetType(List<String> names,
      AnnotationType type) {
    List<DynamicAnnotationConfig> list = new ArrayList<>();
    for (String name : names) {
      DynamicAnnotationConfig dynamicAnnotationConfig = dynamicAnnotations.get(name);
      dynamicAnnotationConfig = (DynamicAnnotationConfig) dynamicAnnotationConfig.clone();
      dynamicAnnotationConfig.setType(type);// nullexception
      list.add(dynamicAnnotationConfig);
    }
    return new DynamicAnnotationMatcherImpl(list);
  }

  private void iniSchema() {
    SchemaRootConfig schemaConfigs = this.getConfig(ConfigEnum.SCHEMA);
    for (SchemaConfig schemaConfig : schemaConfigs.getSchemas()) {
      String defaultDataNode = schemaConfig.getDefaultDataNode();
      String sqlMaxLimit = schemaConfig.getSqlMaxLimit();
      SchemaType schemaType = schemaConfig.getSchemaType();
      RouteStrategy routeStrategy = null;
      switch (schemaType) {
        case DB_IN_ONE_SERVER:
          routeStrategy = new DbInOneServerRouteStrategy();
          break;
        case DB_IN_MULTI_SERVER:
          routeStrategy = new DbInMutilServerRouteStrategy();
          break;
        case ANNOTATION_ROUTE:
          routeStrategy = new AnnotationRouteStrategy();
          break;
        case SQL_PARSE_ROUTE:
          routeStrategy = new SqlParseRouteRouteStrategy();
          break;
      }
      MycatSchema schema = new MycatSchema(schemaConfig, routeStrategy);
      if (sqlMaxLimit != null && !"".equals(sqlMaxLimit)) {
        schema.setSqlMaxLimit(Long.parseLong(sqlMaxLimit));
      }
      if (defaultDataNode != null && !"".equals(defaultDataNode)) {
        schema.setDefaultDataNode(defaultDataNode);
      }
      List<TableDefConfig> tables = schemaConfig.getTables();
      if (tables != null) {
        Map<String, MycatTable> mycatTables = new HashMap<>();
        for (TableDefConfig tableConfig : tables) {
          String dataNodeText = tableConfig.getDataNodes();
          String tableName = tableConfig.getName();

          String tableRuleName = tableConfig.getTableRule();
          MycatTableRule tableRule = getTableRuleByTableName(tableName);
          List<String> dataNodes = Collections.EMPTY_LIST;

          if (!StringUtil.isEmpty(dataNodeText)) {
            dataNodes = Arrays.asList(SplitUtil.split(dataNodeText, ","));
          }
          MycatTable table;

          if (tableConfig.getType() != null) {
            switch (tableConfig.getType()) {
              case GLOBAL:
                mycatTables.put(tableName, table = new GlobalTable(schema, tableConfig, dataNodes));
                break;
              case SHARING_DATABASE:
                mycatTables
                    .put(tableName,
                        table = new ShardingDbTable(schema, tableConfig, dataNodes, tableRule));
                break;
              case SHARING_TABLE:
                mycatTables.put(tableName,
                    table = new ShardingTableTable(schema, tableConfig, dataNodes, tableRule));
                break;
              case SHARING_DATABASE_TABLE:
                mycatTables
                    .put(tableName,
                        table = new ShardingDatabseTableTable(schema, tableConfig, dataNodes,
                            tableRule));
                break;
              case ER:
                mycatTables
                    .put(tableName, table = new ERTable(schema, tableConfig, dataNodes, tableRule));
                break;
              default:
                throw new MycatExpection("");
            }
          } else {
            mycatTables
                .put(tableName,
                    table = new DefaultTable(schema, tableConfig, dataNodes));
          }

        }
        schema.setTables(mycatTables);
        schemas.put(schemaConfig.getName(), schema);
      }
    }
  }

  private void initTableRule() {
    MycatRouterConfig mycatRouter = this;
    ShardingRuleRootConfig rule = getConfig(ConfigEnum.RULE);
    if (rule == null) {
      return;
    }
    initSQLinterceptor(rule);
    for (SharingTableRule tableRule : rule.getTableRules()) {
      String name = tableRule.getName();
      Route rootRouteNode = null;
      Route routeNode = null;
      List<ShardingRule> rules = tableRule.getRules();
      List<DynamicAnnotationConfig> list = new ArrayList<>();
      RuleAlgorithm algorithm = null;
      for (ShardingRule shardingRule : rules) {
        List<String> equal = shardingRule.getEqualAnnotations();
        if (equal != null) {
          list.addAll(getDynamicAnnotationConfigList(shardingRule.getEqualAnnotations(),
              AnnotationType.SHARDING_EQUAL));
        }
        List<String> range = shardingRule.getRangeAnnotations();
        if (range != null) {
          list.addAll(getDynamicAnnotationConfigList(range, AnnotationType.SHARDING_RANGE));
        }
        String column = shardingRule.getColumn();
        HashSet<String> equalsKey = new HashSet<>(
            Arrays.asList(SplitUtil.split(shardingRule.getEqualKeys(), ",")));
        HashSet<String> rangeStartKey = new HashSet<>(
            Arrays.asList(SplitUtil.split(shardingRule.getRangeStartKey(), ",")));
        HashSet<String> rangeEndKey = new HashSet<>(
            Arrays.asList(SplitUtil.split(shardingRule.getRangeEndKey(), ",")));
        Route tmp = new Route(column, equalsKey, rangeStartKey, rangeEndKey);
        if (rootRouteNode == null) {
          String funtion = tableRule.getFuntion();
          algorithm = mycatRouter.getRuleAlgorithm(funtion);
          routeNode = rootRouteNode = tmp;
        } else {
          routeNode.setNextRoute(tmp);
          routeNode = tmp;
        }
      }
      DynamicAnnotationMatcherImpl matcher;
      if (!list.isEmpty()) {
        matcher = new DynamicAnnotationMatcherImpl(list);
      } else {
        matcher = DynamicAnnotationMatcherImpl.EMPTY;
      }
      tableRules.put(name, new MycatTableRule(name, rootRouteNode, algorithm, matcher));
    }
  }


  private static RuleAlgorithm createFunction(String name, String clazz)
      throws ClassNotFoundException, InstantiationException,
                 IllegalAccessException {
    Class<?> clz = Class.forName(clazz);
    //判断是否继承AbstractPartitionAlgorithm
    if (!RuleAlgorithm.class.isAssignableFrom(clz)) {
      throw new IllegalArgumentException("rule function must implements "
                                             + RuleAlgorithm.class.getName() + ", name=" + name);
    }
    return (RuleAlgorithm) clz.newInstance();
  }

  private void initSQLinterceptor(ShardingRuleRootConfig rule) {
    String sqlInterceptorClass = rule.getSqlInterceptorClass();
    if (!(StringUtil.isEmpty(sqlInterceptorClass))) {
      try {
        Class<?> clz = Class.forName(sqlInterceptorClass);
        sqlInterceptor = (SQLInterceptor) clz.newInstance();
      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        e.printStackTrace();
      }
    } else {
      sqlInterceptor = sql -> sql;
    }
  }

  public MycatSchema getSchemaBySchemaName(String name) {
    return schemas.get(name);
  }

  public Collection<MycatSchema> getSchemaList() {
    return schemas.values();
  }

  public MycatDataNode getDataNodeByName(String dataNode) {
    return this.dataNodes.get(dataNode);
  }

  public SQLInterceptor getSqlInterceptor() {
    return sqlInterceptor;
  }
}

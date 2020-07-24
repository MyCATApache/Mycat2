/**
 * Copyright (C) <2020>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat;

import com.alibaba.fastjson.JSONObject;
import io.mycat.config.*;
import io.mycat.util.JsonUtil;
import io.mycat.util.YamlUtil;
import lombok.Builder;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.ToString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;


public class FileConfigProvider implements ConfigProvider {
    volatile MycatConfig config;
    private String defaultPath;
    final AtomicInteger count = new AtomicInteger();
    static final Logger logger = LoggerFactory.getLogger(FileConfigProvider.class);
    private HashMap<String, Object> globalVariables;

    @Override
    public void init(Class rootClass, Map<String, String> config) throws Exception {
        String path = getConfigPath(rootClass, config);
        this.defaultPath = path;
        fetchConfig(this.defaultPath);

        Path resolve = Paths.get(path).getParent().resolve("globalVariables.json");
        Map from = JsonUtil.from(new String(Files.readAllBytes(resolve)), Map.class);
        JSONObject map = (JSONObject) from.get("map");
        this.globalVariables = new HashMap<>();
        map.forEach((key, v) -> {
            JSONObject jsonObject = (JSONObject) v;
            globalVariables.put(key, jsonObject.get("value"));
        });

        //副配置
        this.config = Files.walk(Paths.get(defaultPath).getParent()).filter(i -> {
            Path fileName = i.getFileName();
            return fileName.endsWith(".yml") || fileName.endsWith(".yaml");
        }).distinct()
                .map(i -> {
                    try {
                        return getMycatConfig(i.toString());
                    } catch (Throwable e) {
                        logger.warn("skip:" + i);
                        return null;
                    }
                }).filter(i -> i != null).reduce(this.config, (main, config2) -> {

                    List<ShardingQueryRootConfig.LogicSchemaConfig> logicSchemaConfigs = Optional.ofNullable(config2.getMetadata()).map(i -> i.getSchemas()).orElse(Collections.emptyList());
                    List<PatternRootConfig> patternRootConfigs = Optional.ofNullable(config2.getInterceptors()).orElse(Collections.emptyList());
                    List<DatasourceRootConfig.DatasourceConfig> datasourceConfigs = Optional.ofNullable(config2.getDatasource()).map(i -> i.getDatasources()).orElse(Collections.emptyList());
                    List<ClusterRootConfig.ClusterConfig> clusterConfigs = Optional.ofNullable(config2.getCluster()).map(i -> i.getClusters()).orElse(Collections.emptyList());

                    List<ShardingQueryRootConfig.LogicSchemaConfig> schemas = main.getMetadata().getSchemas();
                    schemas.addAll(logicSchemaConfigs);

                    List<PatternRootConfig> interceptors = main.getInterceptors();
                    interceptors.addAll(patternRootConfigs);

                    List<DatasourceRootConfig.DatasourceConfig> datasources = main.getDatasource().getDatasources();
                    datasources.addAll(datasourceConfigs);

                    List<ClusterRootConfig.ClusterConfig> clusters = main.getCluster().getClusters();
                    clusters.addAll(clusterConfigs);
                    return main;
                });
        List<ShardingQueryRootConfig.LogicSchemaConfig> logicSchemaConfigs = Optional.ofNullable(this.config)
                .map(m -> m.getMetadata())
                .map(m -> m.getSchemas())
                .map(m -> m.stream().filter(i -> i.getGenerator() != null).collect(Collectors.toList()))
                .orElse(Collections.emptyList());
        for (ShardingQueryRootConfig.LogicSchemaConfig logicSchemaConfig : logicSchemaConfigs) {
            Map<String, ShardingTableConfig> shadingTables = logicSchemaConfig.getShadingTables();
            if (shadingTables == null) {
                shadingTables = new HashMap<>();
                logicSchemaConfig.setShadingTables(shadingTables);
            }
            Map<String, GlobalTableConfig> globalTables = logicSchemaConfig.getGlobalTables();
            if (globalTables == null) {
                globalTables = new HashMap<>();
                logicSchemaConfig.setGlobalTables(globalTables);
            }
            ShardingQueryRootConfig.Generator generator = logicSchemaConfig.getGenerator();
            String clazz = generator.getClazz();
            Class<?> aClass = Class.forName(clazz);
            Constructor<?> declaredConstructor = aClass.getDeclaredConstructors()[0];
            TableConfigGenerator tableConfigGenerator = (TableConfigGenerator) declaredConstructor.newInstance(this.config,logicSchemaConfig,generator.getListOptions(),generator.getKvOptions());
            shadingTables.putAll(tableConfigGenerator.generateShardingTable());
            globalTables.putAll(tableConfigGenerator.generateGlobalTable());
        }

        logger.warn("----------------------------------Combined configuration----------------------------------");
        logger.info(YamlUtil.dump(this.config));
}

    /**
     * 根据初始化信息生成配置
     *
     * @param rootClass
     * @param config
     * @return
     * @throws URISyntaxException
     */
    private String getConfigPath(Class rootClass, Map<String, String> config) throws URISyntaxException {
        String path = config.get("path");
        if (path == null) {
            if (rootClass == null) {
                rootClass = FileConfigProvider.class;
            }
            URI uri = rootClass.getResource("/mycat.yml").toURI();
            System.out.println("uri:" + uri);
            path = Paths.get(uri).toAbsolutePath().toString();
        } else {
            System.out.println("path:" + path);
            path = Paths.get(path).resolve("mycat.yml").toAbsolutePath().toString();
        }
        return path;
    }

    @Override
    public void fetchConfig() throws Exception {
        fetchConfig(defaultPath);
    }

    @Override
    public synchronized void report(MycatConfig changed) {
        try {
            backup();
            YamlUtil.dumpToFile(defaultPath, YamlUtil.dump(changed));
            config = changed;
        } catch (Throwable e) {
            logger.error("", e);
        }
    }

    private void backup() {
        try {
            YamlUtil.dumpBackupToFile(defaultPath, count.getAndIncrement(), YamlUtil.dump(config));
        } catch (Exception e) {
            logger.error("", e);
        }
    }


    @Override
    public void fetchConfig(String url) throws Exception {
        MycatConfig config = getMycatConfig(url);
        this.config = config;
    }

    @SneakyThrows
    private static MycatConfig getMycatConfig(String url) {
        Path asbPath = Paths.get(url).toAbsolutePath();
        if (!Files.exists(asbPath)) {
            throw new IllegalArgumentException(MessageFormat.format("path not found: {0}", Objects.toString(asbPath)));
        }
        Iterator<String> iterator = Files.lines(asbPath).iterator();
        StringBuilder sqlGroups = new StringBuilder();
        StringBuilder full = new StringBuilder();
        boolean in = false;
        while (iterator.hasNext()) {
            String next = iterator.next();
            if (next.startsWith("#lib start")) {
                sqlGroups.append(next).append('\n');
                in = true;
            } else if (in) {
                sqlGroups.append(next).append('\n');
            } else if (next.startsWith("#lib end")) {
                sqlGroups.append(next).append('\n');
                in = false;
            } else {
                full.append(next).append('\n');
            }
        }
        sqlGroups.append(full);
        System.out.println(sqlGroups);
        return YamlUtil.loadText(sqlGroups.toString(), MycatConfig.class);
    }


    @Override
    public MycatConfig currentConfig() {
        return config;
    }

    @Override
    public Map<String, Object> globalVariables() {
        return globalVariables;
    }

    @Override
    public synchronized void reportReplica(String replicaName, List<String> dataSourceList) {
        try {
            Path resolve = Paths.get(defaultPath).getParent().resolve("replica.log");
            StringBuilder outputStreamWriter = new StringBuilder();
            outputStreamWriter.append(ReplicaInfo.builder().replicaName(replicaName).dataSourceList(dataSourceList).build());
            outputStreamWriter.append("\n");
            logger.error("switch log: {}", outputStreamWriter.toString());
            Files.write(resolve, outputStreamWriter.toString().getBytes(), StandardOpenOption.APPEND, StandardOpenOption.CREATE);
        } catch (Throwable e) {
            logger.error("", e);
        }
    }

    public String getDefaultPath() {
        return defaultPath;
    }

@Getter
@Builder
@ToString
static class ReplicaInfo {
    String replicaName;
    List<String> dataSourceList;
}
}
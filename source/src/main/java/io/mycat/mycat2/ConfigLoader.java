package io.mycat.mycat2;

import io.mycat.mycat2.beans.ReplicaConfBean;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.mycat2.beans.SchemaConfBean;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.YamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Desc:
 *
 * @date: 13/09/2017
 * @author: gaozhiwen
 */
public class ConfigLoader {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigLoader.class);
    public static final ConfigLoader INSTANCE = new ConfigLoader();

    public static final String DIR_PREPARE = "prepare" + File.separator;
    public static final String DIR_ARCHIVE = "archive" + File.separator;

    private ConfigLoader() {}

    public void loadAll() throws IOException {
        MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        // 保证文件夹存在
        YamlUtil.createDirectoryIfNotExists(DIR_PREPARE);
        YamlUtil.createDirectoryIfNotExists(DIR_ARCHIVE);

        loadReplicaIndex(conf, ConfigEnum.REPLICA_INDEX, null);
        loadDatasource(conf, ConfigEnum.DATASOURCE, null);
        loadSchema(conf, ConfigEnum.SCHEMA, null);

        // 清空prepare文件夹
        YamlUtil.clearDirectory(DIR_PREPARE, null);
    }

    public void load(ConfigEnum configEnum, Integer targetVersion) throws IOException {
        MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        switch (configEnum) {
            case REPLICA_INDEX:
                loadReplicaIndex(conf, ConfigEnum.REPLICA_INDEX, targetVersion);
                YamlUtil.clearDirectory(DIR_PREPARE, ConfigEnum.REPLICA_INDEX.getFileName());
                break;
            case DATASOURCE:
                loadDatasource(conf, ConfigEnum.DATASOURCE, targetVersion);
                YamlUtil.clearDirectory(DIR_PREPARE, ConfigEnum.DATASOURCE.getFileName());
                break;
            case SCHEMA:
                loadSchema(conf, ConfigEnum.SCHEMA, targetVersion);
                YamlUtil.clearDirectory(DIR_PREPARE, ConfigEnum.SCHEMA.getFileName());
                break;
            case SHARDING_RULE:
                break;
            default:
                return;
        }
    }

    /**
     * replica-index.yml
     */
    private void loadReplicaIndex(MycatConfig conf, ConfigEnum configEnum, Integer targetVersion) throws IOException {
        // 加载replica-index
        String fileName = configEnum.getFileName();
        byte configType = configEnum.getType();

        Integer repVersion = YamlUtil.archive(fileName, conf.getConfigVersion(configType), targetVersion);
        if (repVersion == null) {
            return;
        }
        LOGGER.debug("load config for {}", configEnum);
        ReplicaIndexBean replicaIndexBean = YamlUtil.load(fileName, ReplicaIndexBean.class);
        conf.addRepIndex(replicaIndexBean);
        conf.putConfig(configType, replicaIndexBean, repVersion);
    }

    /**
     * datasource.yml
     */
    private void loadDatasource(MycatConfig conf, ConfigEnum configEnum, Integer targetVersion) throws IOException {
        // 加载datasource
        String fileName = configEnum.getFileName();
        byte configType = configEnum.getType();

        Integer dsVersion = YamlUtil.archive(fileName, conf.getConfigVersion(configType), targetVersion);
        if (dsVersion == null) {
            return;
        }
        LOGGER.debug("load config for {}", configEnum);
        ReplicaConfBean replicaConfBean = YamlUtil.load(fileName, ReplicaConfBean.class);
        replicaConfBean.getMysqlReplicas().forEach(replicaBean -> conf.addMySQLRepBean(replicaBean));
        conf.putConfig(configType, replicaConfBean, dsVersion);
    }

    /**
     * schema.yml
     */
    private void loadSchema(MycatConfig conf, ConfigEnum configEnum, Integer targetVersion) throws IOException {
        // 加载schema
        String fileName = configEnum.getFileName();
        byte configType = configEnum.getType();

        Integer schemaVersion = YamlUtil.archive(fileName, conf.getConfigVersion(configType), targetVersion);
        if (schemaVersion == null) {
            return;
        }
        LOGGER.debug("load config for {}", configEnum);
        SchemaConfBean schemaConfBean = YamlUtil.load(fileName, SchemaConfBean.class);
        schemaConfBean.getSchemas().forEach(schemaBean -> conf.addSchemaBean(schemaBean));
        conf.putConfig(configType, schemaConfBean, schemaVersion);
    }
}

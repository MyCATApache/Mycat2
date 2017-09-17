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

    public static final String DIR_CONF = "conf" + File.separator;
    public static final String DIR_PREPARE = "prepare" + File.separator;
    public static final String DIR_ARCHIVE = "archive" + File.separator;

    private ConfigLoader() {}

    public void loadAll() throws IOException {
        MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        // 保证文件夹存在
        YamlUtil.createDirectoryIfNotExists(DIR_PREPARE);
        YamlUtil.createDirectoryIfNotExists(DIR_ARCHIVE);

        loadReplicaIndex(false, conf, ConfigEnum.REPLICA_INDEX, null);
        loadDatasource(false, conf, ConfigEnum.DATASOURCE, null);
        loadSchema(false, conf, ConfigEnum.SCHEMA, null);

        // 清空prepare文件夹
        YamlUtil.clearDirectory(DIR_PREPARE, null);
    }

    public void load(ConfigEnum configEnum, Integer targetVersion) throws IOException {
        MycatConfig conf = (MycatConfig) ProxyRuntime.INSTANCE.getProxyConfig();
        switch (configEnum) {
            case REPLICA_INDEX:
                loadReplicaIndex(true, conf, ConfigEnum.REPLICA_INDEX, targetVersion);
                YamlUtil.clearDirectory(DIR_PREPARE, ConfigEnum.REPLICA_INDEX.getFileName());
                break;
            case DATASOURCE:
                loadDatasource(true, conf, ConfigEnum.DATASOURCE, targetVersion);
                YamlUtil.clearDirectory(DIR_PREPARE, ConfigEnum.DATASOURCE.getFileName());
                break;
            case SCHEMA:
                loadSchema(true, conf, ConfigEnum.SCHEMA, targetVersion);
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
    private void loadReplicaIndex(boolean needAchive, MycatConfig conf, ConfigEnum configEnum, Integer targetVersion) throws IOException {
        // 加载replica-index
        String fileName = configEnum.getFileName();
        byte configType = configEnum.getType();

        Integer curVersion = conf.getConfigVersion(configType);
        if (needAchive) {
            Integer repVersion = YamlUtil.archive(fileName, curVersion, targetVersion);
            if (repVersion == null) {
                return;
            }
            curVersion = repVersion;
        }
        LOGGER.debug("load config for {}", configEnum);
        ReplicaIndexBean replicaIndexBean = YamlUtil.load(fileName, ReplicaIndexBean.class);
        conf.addRepIndex(replicaIndexBean);
        conf.putConfig(configType, replicaIndexBean, curVersion);
    }

    /**
     * datasource.yml
     */
    private void loadDatasource(boolean needAchive, MycatConfig conf, ConfigEnum configEnum, Integer targetVersion) throws IOException {
        // 加载datasource
        String fileName = configEnum.getFileName();
        byte configType = configEnum.getType();

        Integer curVersion = conf.getConfigVersion(configType);
        if (needAchive) {
            Integer dsVersion = YamlUtil.archive(fileName, curVersion, targetVersion);
            if (dsVersion == null) {
                return;
            }
            curVersion = dsVersion;
        }
        LOGGER.debug("load config for {}", configEnum);
        ReplicaConfBean replicaConfBean = YamlUtil.load(fileName, ReplicaConfBean.class);
        replicaConfBean.getMysqlReplicas().forEach(replicaBean -> conf.addMySQLRepBean(replicaBean));
        conf.putConfig(configType, replicaConfBean, curVersion);
    }

    /**
     * schema.yml
     */
    private void loadSchema(boolean needAchive, MycatConfig conf, ConfigEnum configEnum, Integer targetVersion) throws IOException {
        // 加载schema
        String fileName = configEnum.getFileName();
        byte configType = configEnum.getType();

        Integer curVersion = conf.getConfigVersion(configType);
        if (needAchive) {
            Integer schemaVersion = YamlUtil.archive(fileName, curVersion, targetVersion);
            if (schemaVersion == null) {
                return;
            }
            curVersion = schemaVersion;
        }
        LOGGER.debug("load config for {}", configEnum);
        SchemaConfBean schemaConfBean = YamlUtil.load(fileName, SchemaConfBean.class);
        schemaConfBean.getSchemas().forEach(schemaBean -> conf.addSchemaBean(schemaBean));
        conf.putConfig(configType, schemaConfBean, curVersion);
    }
}

package io.mycat.mycat2;

import io.mycat.mycat2.beans.ReplicaConfBean;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.mycat2.beans.SchemaConfBean;
import io.mycat.proxy.ConfigEnum;
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

    public void loadAll(MycatConfig conf) throws IOException {
        // 保证文件夹存在
        YamlUtil.createDirectoryIfNotExists(DIR_PREPARE);
        YamlUtil.createDirectoryIfNotExists(DIR_ARCHIVE);

        loadReplicaIndex(conf);
        loadDatasource(conf);
        loadSchema(conf);

        // 清空prepare文件夹
        YamlUtil.clearDirectory(DIR_PREPARE);
    }

    /**
     * replica-index.yml
     */
    public void loadReplicaIndex(MycatConfig conf) throws IOException {
        // 加载replica-index
        Integer repVersion = YamlUtil.archive(ConfigEnum.REPLICA_INDEX.getFileName(), conf.getConfigVersion(ConfigEnum.REPLICA_INDEX.getType()));
        LOGGER.debug("load config for {}", ConfigEnum.REPLICA_INDEX.getFileName());
        ReplicaIndexBean replicaIndexBean = YamlUtil.load(ConfigEnum.REPLICA_INDEX.getFileName(), ReplicaIndexBean.class);
        conf.addRepIndex(replicaIndexBean);
        conf.putConfig(ConfigEnum.REPLICA_INDEX.getType(), replicaIndexBean, repVersion);
    }

    /**
     * datasource.yml
     */
    public void loadDatasource(MycatConfig conf) throws IOException {
        // 加载datasource
        Integer dsVersion = YamlUtil.archive(ConfigEnum.DATASOURCE.getFileName(), conf.getConfigVersion(ConfigEnum.DATASOURCE.getType()));
        LOGGER.debug("load config for {}", ConfigEnum.DATASOURCE.getFileName());
        ReplicaConfBean replicaConfBean = YamlUtil.load(ConfigEnum.DATASOURCE.getFileName(), ReplicaConfBean.class);
        replicaConfBean.getMysqlReplicas().forEach(replicaBean -> conf.addMySQLRepBean(replicaBean));
        conf.putConfig(ConfigEnum.DATASOURCE.getType(), replicaConfBean, dsVersion);
    }

    /**
     * schema.yml
     */
    public void loadSchema(MycatConfig conf) throws IOException {
        // 加载schema
        Integer schemaVersion = YamlUtil.archive(ConfigEnum.SCHEMA.getFileName(), conf.getConfigVersion(ConfigEnum.SCHEMA.getType()));
        LOGGER.debug("load config for {}", ConfigEnum.SCHEMA.getFileName());
        SchemaConfBean schemaConfBean = YamlUtil.load(ConfigEnum.SCHEMA.getFileName(), SchemaConfBean.class);
        schemaConfBean.getSchemas().forEach(schemaBean -> conf.addSchemaBean(schemaBean));
        conf.putConfig(ConfigEnum.SCHEMA.getType(), schemaConfBean, schemaVersion);
    }
}

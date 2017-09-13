package io.mycat.mycat2;

import io.mycat.mycat2.beans.ReplicaConfBean;
import io.mycat.mycat2.beans.ReplicaIndexBean;
import io.mycat.mycat2.beans.SchemaConfBean;
import io.mycat.proxy.ConfigEnum;
import io.mycat.util.YamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
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

    private ConfigLoader() {}

    public void loadAll(MycatConfig conf) throws IOException {
        loadReplicaIndex(conf);
        loadDatasource(conf);
        loadSchema(conf);
    }

    /**
     * replica-index.yml
     */
    public void loadReplicaIndex(MycatConfig conf) throws IOException {
        // 加载replica-index
        Integer repVersion = YamlUtil.archive(ConfigEnum.REPLICA_INDEX.getFileName(), conf.getConfigVersion(ConfigEnum.REPLICA_INDEX.getCode()));
        LOGGER.debug("load config for {}", ConfigEnum.REPLICA_INDEX.getFileName());
        ReplicaIndexBean replicaIndexBean = YamlUtil.load(ConfigEnum.REPLICA_INDEX.getFileName(), ReplicaIndexBean.class);
        conf.addRepIndex(replicaIndexBean);
        conf.putConfig(ConfigEnum.REPLICA_INDEX.getCode(), replicaIndexBean, repVersion);
    }

    /**
     * datasource.yml
     */
    public void loadDatasource(MycatConfig conf) throws IOException {
        // 加载datasource
        Integer dsVersion = YamlUtil.archive(ConfigEnum.DATASOURCE.getFileName(), conf.getConfigVersion(ConfigEnum.DATASOURCE.getCode()));
        LOGGER.debug("load config for {}", ConfigEnum.DATASOURCE.getFileName());
        ReplicaConfBean replicaConfBean = YamlUtil.load(ConfigEnum.DATASOURCE.getFileName(), ReplicaConfBean.class);
        replicaConfBean.getMysqlReplicas().forEach(replicaBean -> conf.addMySQLRepBean(replicaBean));
        conf.putConfig(ConfigEnum.DATASOURCE.getCode(), replicaConfBean, dsVersion);
    }

    /**
     * schema.yml
     */
    public void loadSchema(MycatConfig conf) throws IOException {
        // 加载schema
        Integer schemaVersion = YamlUtil.archive(ConfigEnum.SCHEMA.getFileName(), conf.getConfigVersion(ConfigEnum.SCHEMA.getCode()));
        LOGGER.debug("load config for {}", ConfigEnum.SCHEMA.getFileName());
        SchemaConfBean schemaConfBean = YamlUtil.load(ConfigEnum.SCHEMA.getFileName(), SchemaConfBean.class);
        schemaConfBean.getSchemas().forEach(schemaBean -> conf.addSchemaBean(schemaBean));
        conf.putConfig(ConfigEnum.SCHEMA.getCode(), schemaConfBean, schemaVersion);
    }
}

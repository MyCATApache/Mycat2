package io.mycat.mycat2;

import io.mycat.mycat2.sqlannotations.AnnotationProcessor;
import io.mycat.proxy.ConfigEnum;
import io.mycat.proxy.Configurable;
import io.mycat.proxy.ProxyRuntime;
import io.mycat.util.YamlUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * Desc: 配置文件加载类
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

    public void loadCore() throws IOException {
        loadConfig(false, ConfigEnum.PROXY, null);
        loadConfig(false, ConfigEnum.HEARTBEAT, null);
        loadConfig(false, ConfigEnum.CLUSTER, null);
        loadConfig(false, ConfigEnum.BALANCER, null);
    }

    public void loadAll() throws IOException {
        // 保证文件夹存在
        YamlUtil.createDirectoryIfNotExists(DIR_PREPARE);
        YamlUtil.createDirectoryIfNotExists(DIR_ARCHIVE);

        loadConfig(false, ConfigEnum.REPLICA_INDEX, null);
        loadConfig(false, ConfigEnum.DATASOURCE, null);
        loadConfig(false, ConfigEnum.SCHEMA, null);

        // 清空prepare文件夹
        YamlUtil.clearDirectory(DIR_PREPARE, null);
        AnnotationProcessor.getInstance();//强制初始化动态注解
    }

    /**
     * 加载指定配置文件
     * @param needAchive 是否需要归档
     * @param configEnum 配置文件的枚举
     * @param targetVersion 指定的版本号
     * @throws IOException
     */
    public void loadConfig(boolean needAchive, ConfigEnum configEnum, Integer targetVersion) throws IOException {
        // 加载replica-index
        String fileName = configEnum.getFileName();
        MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();

        if (needAchive) {
            Integer curVersion = conf.getConfigVersion(configEnum);
            Integer repVersion = YamlUtil.archive(fileName, curVersion, targetVersion);
            if (repVersion == null) {
                return;
            }
            targetVersion = repVersion;
        }

        LOGGER.debug("load config for {}", configEnum);
        conf.putConfig(configEnum, (Configurable) YamlUtil.load(fileName, configEnum.getClazz()), targetVersion);

        if (needAchive) {
            // 清空prepare下的文件
            YamlUtil.clearDirectory(DIR_PREPARE, fileName);
        }
    }
}

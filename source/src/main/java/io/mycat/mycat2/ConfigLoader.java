package io.mycat.mycat2;

import io.mycat.mycat2.beans.GlobalBean;
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
        loadConfig(ConfigEnum.PROXY, GlobalBean.INIT_VERSION);
        loadConfig(ConfigEnum.HEARTBEAT, GlobalBean.INIT_VERSION);
        loadConfig(ConfigEnum.CLUSTER, GlobalBean.INIT_VERSION);
        loadConfig(ConfigEnum.BALANCER, GlobalBean.INIT_VERSION);
        loadConfig(ConfigEnum.USER, GlobalBean.INIT_VERSION);
    }

    public void loadAll() throws IOException {
        // 保证文件夹存在
        YamlUtil.createDirectoryIfNotExists(DIR_PREPARE);
        YamlUtil.createDirectoryIfNotExists(DIR_ARCHIVE);

        loadConfig(ConfigEnum.REPLICA_INDEX, GlobalBean.INIT_VERSION);
        loadConfig(ConfigEnum.DATASOURCE, GlobalBean.INIT_VERSION);
        loadConfig(ConfigEnum.SCHEMA, GlobalBean.INIT_VERSION);

        // 清空prepare文件夹
        YamlUtil.clearDirectory(DIR_PREPARE, null);
        //AnnotationProcessor.getInstance();//强制初始化动态注解
    }

    /**
     * 加载指定的配置文件
     * @param configEnum 加载的配置枚举值
     * @param version 当前加载的文件版本
     */
    public void loadConfig(ConfigEnum configEnum, int version) throws IOException {
        String fileName = configEnum.getFileName();
        MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();
        LOGGER.info("load config for {}", configEnum);
        conf.putConfig(configEnum, (Configurable) YamlUtil.load(fileName, configEnum.getClazz()), version);
    }

    /**
     * 将当前的配置文件归档，并从prepare中获取指定版本的配置文件作为当前的配置文件，同时清空prepare文件夹
     * @param configEnum
     * @param version
     * @throws IOException
     */
    public void archiveAndLoadConfig(ConfigEnum configEnum, int version) throws IOException {
        String fileName = configEnum.getFileName();
        MycatConfig conf = ProxyRuntime.INSTANCE.getConfig();

        int curVersion = conf.getConfigVersion(configEnum);
        if (YamlUtil.archive(fileName, curVersion, version)) {
            loadConfig(configEnum, version);
        }

        // 清空prepare下的文件
        YamlUtil.clearDirectory(DIR_PREPARE, fileName);
    }
}

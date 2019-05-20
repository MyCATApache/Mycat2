/**
 * Copyright (C) <2019>  <gaozhiwen>
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public void loadProxy(String root, ConfigReceiver receiver) throws IOException {
    loadConfig(root, ConfigEnum.PROXY, GlobalConfig.INIT_VERSION, receiver);
    loadConfig(root, ConfigEnum.PLUG, GlobalConfig.INIT_VERSION, receiver);
    loadConfig(root, ConfigEnum.REPLICA_INDEX, GlobalConfig.INIT_VERSION, receiver);
  }

  public void loadMycat(String root, ConfigReceiver receiver) throws IOException {
    // 保证文件夹存在
    YamlUtil.createDirectoryIfNotExists(DIR_PREPARE);
    YamlUtil.createDirectoryIfNotExists(DIR_ARCHIVE);
    loadConfig(root, ConfigEnum.USER, GlobalConfig.INIT_VERSION, receiver);
    loadConfig(root, ConfigEnum.DATASOURCE, GlobalConfig.INIT_VERSION, receiver);
    loadConfig(root, ConfigEnum.SCHEMA, GlobalConfig.INIT_VERSION, receiver);
    loadConfig(root, ConfigEnum.DYNAMIC_ANNOTATION, GlobalConfig.INIT_VERSION, receiver);
    // 清空prepare文件夹
    YamlUtil.clearDirectory(DIR_PREPARE, null);
  }

  /**
   * 加载指定的配置文件
   * @param configEnum 加载的配置枚举值
   * @param version 当前加载的文件版本
   */

  public void loadConfig(String root, ConfigEnum configEnum, int version, ConfigReceiver receiver) {
    try {
      Path fileName = Paths.get(root).resolve(configEnum.getFileName()).toAbsolutePath();

      if (Files.exists(fileName)) {
        LOGGER.info("load config for {}", configEnum);
        receiver.putConfig(configEnum,
            (Configurable) YamlUtil.load(fileName.toString(), configEnum.getClazz()), version);
        return;
      }
    }catch (Exception e){
      e.printStackTrace();
      LOGGER.error("load config for {} fail",e);
    }
  }

  /**
   * 将当前的配置文件归档，并从prepare中获取指定版本的配置文件作为当前的配置文件，同时清空prepare文件夹
   *
   * @param configEnum
   * @param version
   * @throws IOException
   */
  public void archiveAndLoadConfig(String root, ConfigEnum configEnum, int version,
      ConfigReceiver receiver)
      throws IOException {
    String fileName = configEnum.getFileName();
    int curVersion = receiver.getConfigVersion(configEnum);
    if (YamlUtil.archive(fileName, curVersion, version)) {
      loadConfig(root, configEnum, version, receiver);
    }
    // 清空prepare下的文件
    YamlUtil.clearDirectory(DIR_PREPARE, fileName);
  }
}

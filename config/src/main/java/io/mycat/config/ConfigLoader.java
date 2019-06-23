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
 * date: 13/09/2017
 *
 * @author: gaozhiwen junwen
 */
public class ConfigLoader {

  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigLoader.class);
  public static final String DIR_CONF = "conf" + File.separator;
  public static final String DIR_PREPARE = "prepare" + File.separator;
  public static final String DIR_ARCHIVE = "archive" + File.separator;

  public static ConfigReceiver load(String root) throws IOException {
    return load(root, GlobalConfig.genVersion());
  }

  public static ConfigReceiver load(String root, int version) throws IOException {
    ConfigReceiver cr = new ConfigReceiverImpl(root,version);
    for (ConfigEnum value : ConfigEnum.values()) {
      loadConfig(root, value, cr);
    }
    return cr;
  }

  /**
   * 加载指定的配置文件
   *
   * @param configEnum 加载的配置枚举值
   */

  public static void loadConfig(String root, ConfigEnum configEnum, ConfigReceiver receiver) {
    try {
      Path fileName = Paths.get(root).resolve(configEnum.getFileName()).toAbsolutePath();

      if (Files.exists(fileName)) {
        LOGGER.info("load config for {}", configEnum);
        String path = fileName.toString();
        ConfigurableRoot load = (ConfigurableRoot) YamlUtil
            .load(path, configEnum.getClazz());
        load.setFilePath(path);
        receiver.putConfig(configEnum,load);
        return;
      }
    } catch (Exception e) {
      e.printStackTrace();
      LOGGER.error("load config for {} fail", e);
    }
    LOGGER.warn(root + "/" + configEnum.getFileName() + "not exist");
  }
}

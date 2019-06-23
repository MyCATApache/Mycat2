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

import io.mycat.config.route.DynamicAnnotationConfig;
import io.mycat.config.route.DynamicAnnotationRootConfig;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

/**
 * Desc: yml文件的工具类
 *
 * date: 09/09/2017
 *
 * @author: gaozhiwen
 */
public class YamlUtil {

  private static final Logger LOGGER = LoggerFactory.getLogger(YamlUtil.class);

  /**
   * 从指定的文件中加载配置
   */
  public static <T> T load(String fileName, Class<T> clazz) throws FileNotFoundException {
    InputStreamReader fis = null;
    try {
      URL url = YamlUtil.class.getClassLoader().getResource(fileName);
      if (url == null) {
        url = Paths.get(fileName).toAbsolutePath().toUri().toURL();
      }
      if (url != null) {
        Yaml yaml = new Yaml();
        fis = new InputStreamReader(new FileInputStream(url.getFile()), StandardCharsets.UTF_8);
        T obj = yaml.loadAs(fis, clazz);
        return obj;
      }
      return null;
    } catch (MalformedURLException e) {
      throw new FileNotFoundException(fileName);
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException ignored) {
        }
      }
    }
  }

  /**
   * 将对象dump成yaml格式的字符串
   */
  public static String dump(Object obj) {
    DumperOptions options = new DumperOptions();
    options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
    Representer representer = new Representer();
    representer.addClassTag(obj.getClass(), Tag.MAP);
    Yaml yaml = new Yaml(representer, options);
    return yaml.dump(obj);
  }

  public static void main(String[] args) {
    DynamicAnnotationRootConfig rootConfig = new DynamicAnnotationRootConfig();
    rootConfig.setDynamicAnnotations(new ArrayList<>());
    List<DynamicAnnotationConfig> dynamicAnnotationConfig = rootConfig.getDynamicAnnotations();
    DynamicAnnotationConfig dynamicAnnotation = new DynamicAnnotationConfig();
    dynamicAnnotationConfig.add(dynamicAnnotation);
    dynamicAnnotation.setName("expr");
    dynamicAnnotation.setPattern("sssss");
    String dump = dump(rootConfig);
  }

  /**
   * 将对象dump成yaml格式并保存成指定文件，文件名格式：confName + "-" + version，如mycat.yml-1
   */
  public static void dumpBackupToFile(String confName, int version, String content) {
    String fileName = getBackupFileName(confName, version);
    dumpToFile(fileName, content);
  }

  public static void dumpToFile(String confName, String content) {
    try (FileWriter writer = new FileWriter(confName)) {
      writer.write(content);
    } catch (IOException e) {
      LOGGER.error("error to write content: {} to path: {}", content, confName, e);
    }
  }

  public static String getBackupFileName(String configName, int version) {
    return configName + "-" + version + LocalTime.now();
  }
}

/**
 * Copyright (C) <2019>  <gaozhiwen>
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
package io.mycat.util;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.nodes.Tag;
import org.yaml.snakeyaml.representer.Representer;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.text.MessageFormat;

/**
 * Desc: yml文件的工具类
 *
 * date: 09/09/2017
 *
 * @author: gaozhiwen
 */
public class YamlUtil {


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
                fis = new InputStreamReader(new FileInputStream(url.getFile()), StandardCharsets.UTF_8);
                return load(clazz, fis);
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
    public static <T> T load(Class<T> clazz, Reader fis) {
        return   new Yaml().loadAs(fis, clazz);
    }
    public static <T> T loadText(String text,Class<T> clazz) {
        return   new Yaml().loadAs(text, clazz);
    }


    /**
     * 将对象dump成yaml格式的字符串
     */
    public static String dump(Object obj) {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.AUTO);
        options.setPrettyFlow(false);
        Representer representer = new Representer();
        representer.addClassTag(obj.getClass(), Tag.MAP);
        Yaml yaml = new Yaml(representer, options);
        return yaml.dump(obj);
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
            System.err.println(MessageFormat.format("error to write content: {0} to path: {1} ,{2}", content, confName, e.toString()));
        }
    }

    public static String getBackupFileName(String configName, int version) {
        return configName + "-" + version;
    }
}

package io.mycat.util;

import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;

/**
 * Desc: yml文件的工具类
 *
 * @date: 09/09/2017
 * @author: gaozhiwen
 */
public class YamlUtil {
    public static <T> T load(String fileName, Class<T> clazz) throws FileNotFoundException {
        FileInputStream fis = null;
        try {
            URL url = YamlUtil.class.getClassLoader().getResource(fileName);
            if (url != null) {
                Yaml yaml = new Yaml();
                fis = new FileInputStream(url.getFile());
                T obj = yaml.loadAs(fis, clazz);
                return obj;
            }
            return null;
        } finally {
            if (fis != null) {
                try {
                    fis.close();
                } catch (IOException ignored) {
                }
            }
        }
    }

    public static String dump(Object obj) {
        DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);
        Yaml yaml = new Yaml(options);
        String str = yaml.dump(obj);
        return str;
    }
}

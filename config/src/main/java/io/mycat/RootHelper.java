package io.mycat;

import io.mycat.util.YamlUtil;

import java.net.URI;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public enum RootHelper {
    INSTCANE;

    public ConfigProvider bootConfig(Class rootClass) throws Exception {
        String configProviderKeyName = "MYCAT_CONFIG_PROVIER";
        String className = System.getProperty(configProviderKeyName);

        if (className == null){
            className = FileConfigProvider.class.getName();
        }

        String configResourceKeyName = "MYCAT_HOME";
        String path = System.getProperty(configResourceKeyName);
        if (path == null){
            URI uri = rootClass.getResource("/mycat.yml").toURI();
            path = Paths.get( uri).toAbsolutePath().toString();
        }
        ConfigProvider tmpConfigProvider = null;

        Class<?> clazz = Class.forName(className);
        tmpConfigProvider = (ConfigProvider) clazz.getDeclaredConstructor().newInstance();
        HashMap<String, String> config = new HashMap<>();
        System.out.println(YamlUtil.dump(new MycatConfig()));
        config.put("path", path);
        tmpConfigProvider.init(config);

        return tmpConfigProvider;
    }
}
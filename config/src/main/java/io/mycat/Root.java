package io.mycat;

import io.mycat.util.YamlUtil;

import java.util.HashMap;
import java.util.Map;

public enum Root {
    INSTCANE;
    final ConfigProvider configProvider;

    Root() {
        String configProviderKeyName = "MYCAT_CONFIG_PROVIER";
        String className = System.getProperty(configProviderKeyName);

        String configResourceKeyName = "MYCAT_HOME";
        String path = System.getProperty(configResourceKeyName);
        ConfigProvider tmpConfigProvider = null;
        try {
            Class<?> clazz = Class.forName(className);
            tmpConfigProvider = (ConfigProvider) clazz.getDeclaredConstructor().newInstance();
            HashMap<String, String> config = new HashMap<>();
            config.put("path", path);
            tmpConfigProvider.init(config);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.configProvider = tmpConfigProvider;
    }

    public static void main(String[] args) {
        Map<String,String> c = new HashMap<>();
        c.put("aa","1111111111111");
        String dump = YamlUtil.dump(c);
    }
}
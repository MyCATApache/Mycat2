/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat;

import lombok.SneakyThrows;

import java.util.HashMap;

public enum RootHelper {
    INSTANCE;
    volatile ConfigProvider configProvider;

    @SneakyThrows
    public ConfigProvider getConfigProvider() {
        return bootConfig(null);
    }

    public synchronized ConfigProvider bootConfig(Class rootClass) throws Exception {
        if (configProvider == null) {
            String configProviderKeyName = "MYCAT_CONFIG_PROVIER";
            String className = System.getProperty(configProviderKeyName);

            if (className == null) {
                className = FileConfigProvider.class.getName();
            }
            String configResourceKeyName = "MYCAT_HOME";
            String path = System.getProperty(configResourceKeyName);
            System.out.println("path:"+path);

            ConfigProvider tmpConfigProvider = null;

            Class<?> clazz = Class.forName(className);
            tmpConfigProvider = (ConfigProvider) clazz.getDeclaredConstructor().newInstance();
            HashMap<String, String> config = new HashMap<>();
            config.put("path", path);
            tmpConfigProvider.init(rootClass,config);
            return configProvider = tmpConfigProvider;
        } else {
            return configProvider;
        }
    }
}
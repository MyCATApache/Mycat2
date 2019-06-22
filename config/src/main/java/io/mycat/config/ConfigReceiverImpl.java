package io.mycat.config;

import java.util.EnumMap;
import java.util.Map;

/**
 * @author jamie12221
 *  date 2019-05-03 15:28
 **/
public class ConfigReceiverImpl implements ConfigReceiver {

  // 当前节点所用的配置文件的版本
  private Map<ConfigEnum, Integer> configVersionMap = new EnumMap<>(ConfigEnum.class);
  private Map<ConfigEnum, ConfigurableRoot> configMap = new EnumMap<>(ConfigEnum.class);
  private Map<ConfigEnum, Long> configUpdateTimeMap = new EnumMap<>(ConfigEnum.class);
  @Override
  public int getConfigVersion(ConfigEnum configEnum) {
    Integer oldVersion = configVersionMap.get(configEnum);
    return oldVersion == null ? GlobalConfig.INIT_VERSION : oldVersion;
  }

  @Override
  public void putConfig(ConfigEnum configEnum, ConfigurableRoot config, int version) {
    configMap.put(configEnum, config);
    configVersionMap.put(configEnum, version);
    configUpdateTimeMap.put(configEnum, System.currentTimeMillis());
  }

  @Override
  public void setConfigVersion(ConfigEnum configEnum, int version) {
    configVersionMap.put(configEnum, version);
    configUpdateTimeMap.put(configEnum, System.currentTimeMillis());
  }

  @Override
  public <T extends ConfigurableRoot> T getConfig(ConfigEnum configEnum) {
    return (T) configMap.get(configEnum);
  }
}

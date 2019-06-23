package io.mycat.config;

import java.util.EnumMap;
import java.util.Map;

/**
 * @author jamie12221 date 2019-05-03 15:28
 **/
public class ConfigReceiverImpl implements ConfigReceiver {

  private String resourcePath;
  private final int version;
  private final Map<ConfigEnum, ConfigurableRoot> configMap = new EnumMap<>(ConfigEnum.class);

  public ConfigReceiverImpl(String resourcePath,int version) {
    this.resourcePath = resourcePath;
    this.version = version;
  }

  @Override
  public String getResourcePath() {
    return resourcePath;
  }

  @Override
  public int getConfigVersion() {
    return version;
  }

  @Override
  public void putConfig(ConfigEnum configEnum, ConfigurableRoot config) {
    configMap.put(configEnum, config);
  }

  @Override
  public <T extends ConfigurableRoot> T getConfig(ConfigEnum configEnum) {
    return (T) configMap.get(configEnum);
  }

}

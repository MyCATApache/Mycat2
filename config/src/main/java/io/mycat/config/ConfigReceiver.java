package io.mycat.config;


public interface ConfigReceiver {
    public int getConfigVersion(ConfigEnum configEnum);
    public void putConfig(ConfigEnum configEnum, Configurable config, int version);
    public void setConfigVersion(ConfigEnum configEnum, int version);
    public <T extends Configurable> T getConfig(ConfigEnum configEnum);
}

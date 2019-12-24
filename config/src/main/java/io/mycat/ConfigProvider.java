package io.mycat;

import java.util.Map;

public interface ConfigProvider {
    void init(Map<String,String> config) throws Exception;
    void fetchConfig(String path) throws Exception;
    void fetchConfig() throws Exception;
    void report(Map<String,Object> changed);

    public MycatConfig currentConfig();
}
package io.mycat;

import java.util.Map;

public class DbConfigProvider implements ConfigProvider {
    @Override
    public void init(Map<String, String> config) throws Exception {

    }

    @Override
    public void fetchConfig(String path) throws Exception {

    }

    @Override
    public void fetchConfig() throws Exception {

    }

    @Override
    public void report(MycatConfig changed) {

    }


    @Override
    public MycatConfig currentConfig() {
        return null;
    }
}
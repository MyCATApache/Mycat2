package io.mycat.config;

import io.mycat.util.YamlUtil;
import lombok.SneakyThrows;

import java.io.FileReader;
import java.nio.file.Paths;

public class ServerConfigurationImpl extends ServerConfiguration {

    private final MycatServerConfig mycatServerConfig;

    @SneakyThrows
    public ServerConfigurationImpl(Class rootClass, String path) {
        super(rootClass);
        this.mycatServerConfig = YamlUtil.load(MycatServerConfig.class, new FileReader(Paths.get(path).resolve("server.yml").toString()));
    }

    @Override
    public MycatServerConfig serverConfig() {
        return this.mycatServerConfig;
    }
}
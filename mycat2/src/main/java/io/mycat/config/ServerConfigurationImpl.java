package io.mycat.config;

import io.mycat.util.YamlUtil;
import lombok.SneakyThrows;

import java.io.FileReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

public class ServerConfigurationImpl extends ServerConfiguration {

    private final MycatServerConfig mycatServerConfig;

    @SneakyThrows
    public ServerConfigurationImpl(Class rootClass, String path) {
        super(rootClass);
        Path serverPath = Paths.get(path).resolve("server.json").toAbsolutePath();
        if (Files.exists(serverPath)) {
            this.mycatServerConfig = YamlUtil.load(MycatServerConfig.class, new FileReader(serverPath.toString()));
        } else {
            this.mycatServerConfig = new MycatServerConfig();
        }
    }

    @Override
    public MycatServerConfig serverConfig() {
        return this.mycatServerConfig;
    }
}
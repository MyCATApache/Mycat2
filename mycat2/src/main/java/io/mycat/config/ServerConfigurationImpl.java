package io.mycat.config;

import io.mycat.ConfigReaderWriter;
import lombok.SneakyThrows;

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
            ConfigReaderWriter configReaderWriter = ConfigReaderWriter.getReaderWriterBySuffix("json");
            this.mycatServerConfig = configReaderWriter.transformation(new String(Files.readAllBytes(Paths.get(serverPath.toString()))),MycatServerConfig.class);
        } else {
            this.mycatServerConfig = new MycatServerConfig();
        }
    }

    @Override
    public MycatServerConfig serverConfig() {
        return this.mycatServerConfig;
    }
}
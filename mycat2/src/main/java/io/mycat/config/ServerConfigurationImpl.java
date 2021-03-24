/**
 * Copyright (C) <2021>  <chen junwen>
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
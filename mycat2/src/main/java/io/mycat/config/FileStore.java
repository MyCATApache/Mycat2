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
import org.apache.commons.io.FileUtils;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class FileStore implements CoordinatorMetadataStorageManager.Store {
    private final Path baseDirectory;
    private final String suffix;
    volatile FileLock lock;
    private ConfigReaderWriter readerWriter;

    public FileStore(Path baseDirectory) {
        this.baseDirectory = baseDirectory;
        this.readerWriter = ConfigReaderWriter.getReaderWriterBySuffix("json");
        this.suffix = readerWriter.getSuffixSet().iterator().next();
    }

    @Override
    public void addChangedCallback(CoordinatorMetadataStorageManager.ChangedValueCallback changedCallback) {

    }

    @Override
    @SneakyThrows
    public synchronized void begin() {
        if (lock != null) {
            throw new RuntimeException("has in transcation");
        }
        Path lockFile = baseDirectory.resolve("mycat.lock");
        if (Files.notExists(lockFile)) Files.createFile(lockFile);
        FileChannel lockFileChannel = FileChannel.open(lockFile, StandardOpenOption.WRITE);
        this.lock = Objects.requireNonNull(lockFileChannel.lock());
    }

    @Override
    @SneakyThrows
    public String get(String schema) {
        return new String(
                Files.readAllBytes(baseDirectory.resolve(schema))
        );
    }

    @Override
    @SneakyThrows
    public void set(String schemas, String transformation) {
        Path resolve = baseDirectory.resolve(schemas);
        FileUtils.write(resolve.toFile(), transformation);
    }

    @Override
    @SneakyThrows
    public void set(String schemas, Map<String, String> transformation) {
        Path resolve = baseDirectory.resolve(schemas);
        for (Map.Entry<String, String> stringStringEntry : transformation.entrySet()) {
            FileUtils.write(resolve.resolve(stringStringEntry.getKey()+"."+suffix).toFile(), stringStringEntry.getValue());
        }
    }

    @Override
    @SneakyThrows
    public Map<String, String> getMap(String schemas) {
        Path resolve = baseDirectory.resolve(schemas);
        return Files.list(resolve).collect(Collectors.toMap(k -> {
            String s = k.getFileName().toString();
            int i = s.lastIndexOf('.');
            if (i == -1) {
                return s;
            }
            return s.substring(0, i);
        }, new Function<Path, String>() {
            @Override
            @SneakyThrows
            public String apply(Path i) {
                return new String(Files.readAllBytes(i));
            }
        }));
    }

    @Override
    public void commit() {
        close();
    }

    @Override
    public void close() {
        if (lock != null) {
            FileChannel channel = lock.channel();
            if (channel.isOpen()) {
                try {
                    lock.release();
                } catch (IOException e) {

                }
                try {
                    lock.close();
                } catch (IOException e) {

                }
            }
        }
    }

}

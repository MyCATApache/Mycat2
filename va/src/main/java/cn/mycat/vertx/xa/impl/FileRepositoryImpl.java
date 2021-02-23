/**
 * Copyright [2021] [chen junwen]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.mycat.vertx.xa.impl;

import cn.mycat.vertx.xa.ImmutableCoordinatorLog;
import cn.mycat.vertx.xa.Repository;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.core.json.Json;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class FileRepositoryImpl implements Repository {
    private final static Logger LOGGER = LoggerFactory.getLogger(FileRepositoryImpl.class);
    private static final String FILE_SEPARATOR = String.valueOf(File.separatorChar);
    private final String baseDir;
    private final String suffix;
    private final Vertx vertx;
    private Long timeHandler;

    public FileRepositoryImpl(final String baseDir, final String suffix, Vertx vertx) {
        this.baseDir = baseDir;
        this.suffix = suffix;
        this.vertx = vertx;
    }

    @Override
    public Future<Void> init() {
        this.timeHandler = this.vertx.setPeriodic(TimeUnit.SECONDS.toMillis(5),
                event -> vertx.executeBlocking(event1 -> {
                    try {
                        long now = System.currentTimeMillis();
                        Files.list(Paths.get(baseDir))
                                .filter(path -> !Files.isDirectory(path) && path.toFile().getPath().endsWith(suffix))
                                .filter(path -> {
                                    long l = path.toFile().lastModified();
                                    long ret = TimeUnit.MILLISECONDS.toSeconds(now - l);
                                    return ret > 1;
                                }).forEach(new Consumer<Path>() {
                            @Override
                            public void accept(Path path) {
                                try {
                                    ImmutableCoordinatorLog coordinatorLogEntry = Json.decodeValue(new String(Files.readAllBytes(path)), ImmutableCoordinatorLog.class);
                                    switch (coordinatorLogEntry.computeMinState()) {
                                        case XA_COMMITED:
                                        case XA_ROLLBACKED:
                                            Files.delete(path);
                                            break;
                                    }
                                } catch (Throwable throwable) {

                                }
                            }
                        });
                    } catch (Throwable e) {

                    } finally {
                        event1.tryComplete();
                    }
                }));
        return Future.succeededFuture();
    }

    @Override
    public void put(String xid, ImmutableCoordinatorLog coordinatorLog) {
        Path resolve = getPath(xid);
        try {
            if (!Files.exists(resolve)) {
                Files.createFile(resolve);
            }
            Files.write(resolve,coordinatorLog.toJson().getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path getPath(String id) {
        return Paths.get(baseDir).resolve(id + FILE_SEPARATOR + suffix);
    }

    @Override
    public void remove(String xid) {
        try {
            Files.deleteIfExists(getPath(xid));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ImmutableCoordinatorLog get(String xid) {
        Path path = getPath(xid);
        try {
            return Json.decodeValue(new String(Files.readAllBytes(path)), ImmutableCoordinatorLog.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Future<Collection<String>> getCoordinatorLogsForRecover() {

        try {
            return Future.succeededFuture(Files.list(Paths.get(baseDir))
                    .filter(path -> !Files.isDirectory(path) && path.toFile().getPath().endsWith(suffix))
                    .map(path -> {
                        try {
                            return new String(Files.readAllBytes(path));
                        } catch (IOException e) {
                            throw new RuntimeException(e);

                        }
                    }).map(i -> Json.decodeValue(i, ImmutableCoordinatorLog.class)).map(i->i.getXid()).collect(Collectors.toList()));
        } catch (Throwable throwable) {
            return Future.failedFuture(throwable);
        }

    }

    @Override
    public  Future<Void>  close() {
        if (timeHandler != null) {
            this.vertx.cancelTimer(this.timeHandler);
            timeHandler = null;
        }
        return Future.succeededFuture();
    }
}

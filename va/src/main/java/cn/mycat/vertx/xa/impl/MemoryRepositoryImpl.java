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
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class MemoryRepositoryImpl implements Repository {
    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryRepositoryImpl.class);
    private final Map<String, ImmutableCoordinatorLog> storage;

    public MemoryRepositoryImpl(Map<String, ImmutableCoordinatorLog> storage) {
        this.storage = storage;
    }

    public MemoryRepositoryImpl() {
        this(new ConcurrentHashMap<>());
    }

    private final ReentrantLock lock = new ReentrantLock();
    private volatile boolean closed = true;

    @Override
    public Future<Void> init() {
        if (lock.tryLock()) {
            try {
                if (closed) {
                    closed = false;
                    return Future.succeededFuture();
                }
            }finally {
                lock.unlock();
            }
        }
        return Future.failedFuture(new IllegalArgumentException("not close"));
    }

    @Override
    public void put(String xid, ImmutableCoordinatorLog coordinatorLog) {
        lock.lock();
        try {
            storage.put(xid, coordinatorLog);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public ImmutableCoordinatorLog get(String xid) {
        return storage.get(xid);
    }

    @Override
    public Future<Void> close() {
        lock.lock();
        try {
            storage.clear();
        } finally {
            closed = true;
            lock.unlock();
        }
        return Future.succeededFuture();
    }

    @Override
    public Future<Collection<String>> getCoordinatorLogsForRecover() {
        return Future.succeededFuture(storage.values().stream().map(i->i.getXid()).collect(Collectors.toList()));
    }

    public boolean isClosed() {
        return closed;
    }

    @Override
    public void remove(String xid) {
        lock.lock();
        try {
            ImmutableCoordinatorLog coordinatorLogEntry = storage.get(xid);
            if (coordinatorLogEntry != null) {
                switch (coordinatorLogEntry.computeMinState()) {
                    case XA_COMMITED:
                    case XA_ROLLBACKED:
                        storage.remove(xid);
                        break;
                    default:
                }
            }
        } finally {
            lock.unlock();
        }
    }
}

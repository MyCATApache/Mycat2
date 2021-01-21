/**
 * Copyright [2021] [chen junwen]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.mycat.vertx.xa.impl;

import cn.mycat.vertx.xa.ImmutableCoordinatorLog;
import cn.mycat.vertx.xa.Repository;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class MemoryRepositoryImpl implements Repository {
    private static final Logger LOGGER = LoggerFactory.getLogger(MemoryRepositoryImpl.class);
    private final Map<String, ImmutableCoordinatorLog> storage = new ConcurrentHashMap<>();
    private final ReentrantLock lock = new ReentrantLock();
    private boolean closed = true;

    @Override
    public void init() {
        closed = false;
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
    public void close() {
        lock.lock();
        try {
            storage.clear();
        } finally {
            closed = true;
            lock.unlock();
        }

    }

    @Override
    public Collection<ImmutableCoordinatorLog> getCoordinatorLogs() {
        return storage.values();
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

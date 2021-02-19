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
import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;

import java.util.Collection;

public class XaRepository implements Repository {
    private final static Logger LOGGER = LoggerFactory.getLogger(XaRepository.class);
    final Repository memoryRepository = new MemoryRepositoryImpl();
    final Repository persistenceRepository;

    public XaRepository(Repository persistenceRepository) {
        this.persistenceRepository = persistenceRepository;
    }

    @Override
    public void init() {
        memoryRepository.init();
        persistenceRepository.init();
    }

    @Override
    public void put(String xid, ImmutableCoordinatorLog coordinatorLog) {
        memoryRepository.put(xid, coordinatorLog);
    }

    @Override
    public void remove(String xid) {
        memoryRepository.remove(xid);
    }

    @Override
    public ImmutableCoordinatorLog get(String xid) {
        return memoryRepository.get(xid);
    }

    @Override
    public Future<Collection<ImmutableCoordinatorLog>> getCoordinatorLogsForRecover() {
        return persistenceRepository.getCoordinatorLogsForRecover();
    }

    @Override
    public void close() {
        memoryRepository.close();
        persistenceRepository.close();
    }
}

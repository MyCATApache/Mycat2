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
import io.vertx.core.json.Json;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

public class KvRepositoryImpl implements Repository {
    private static final Logger LOGGER = LoggerFactory.getLogger(KvRepositoryImpl.class);
    private final String key;
    private final Map<String, String> map;


    public KvRepositoryImpl(String key, Map<String, String> map) {
        this.key = key;
        this.map = map;

    }

    @Override
    public Future<Void> init() {
        return Future.succeededFuture();
    }

    @Override
    public void put(String xid, ImmutableCoordinatorLog coordinatorLog) {
        this.map.put(xid,coordinatorLog.toJson());
    }

    @Override
    public void remove(String xid) {
        this.map.remove(xid);
    }

    @Override
    public ImmutableCoordinatorLog get(String xid) {
        String s = this.map.get(xid);
        if (s!=null){
            return Json.decodeValue(s,ImmutableCoordinatorLog.class);
        }
        return null;
    }

    @Override
    public Future<Collection<ImmutableCoordinatorLog>> getCoordinatorLogsForRecover() {
        return Future.succeededFuture(
                map.values().stream().map(i->Json.decodeValue(i, ImmutableCoordinatorLog.class)).collect(Collectors.toList()));
    }

    @Override
    public Future<Void> close() {
        return Future.succeededFuture();
    }
}

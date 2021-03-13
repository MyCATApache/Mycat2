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
package cn.mycat.vertx.xa;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.mysqlclient.MySQLPool;
import io.vertx.sqlclient.SqlConnection;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


public interface MySQLManager {

    Future<SqlConnection> getConnection(String targetName);
    int  getSessionCount(String targetName);
    Future<Map<String, SqlConnection>> getConnectionMap();

    Future<Void> close();

    Future<Map<String,Integer>> computeConnectionUsageSnapshot();

    void setTimer(long delay, Runnable handler);

    public default Future<Map<String, SqlConnection>> getMapFuture(Set<String> keys) {
        ConcurrentHashMap<String, SqlConnection> map = new ConcurrentHashMap<>();
        List<Future<SqlConnection>> futureList = new ArrayList<>();
        Iterator<String> iterator = keys.iterator();
        while (iterator.hasNext()) {
            String element = iterator.next();
            futureList.add(getConnection(element).flatMap(f -> {
                map.put(element, f);
                return Future.succeededFuture(f);
            }));
        }
        return CompositeFuture.all((List) futureList).onComplete(event -> {
            if (event.failed()) {
                map.values().forEach(c -> c.close());
            }
        }).flatMap(c -> Future.succeededFuture(map));
    }

}
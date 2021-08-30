/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.connectionschedule;

import cn.mycat.vertx.xa.XaSqlConnection;
import io.mycat.MycatDataContext;
import io.mycat.TransactionSession;
import io.mycat.newquery.NewMycatConnection;
import io.mycat.util.VertxUtil;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.sqlclient.SqlConnection;

import java.util.HashMap;
import java.util.Map;

public class SequenceSchedulePolicy implements SchedulePolicy {
    final Map<String, Future<NewMycatConnection>> futures = new HashMap<>();

    @Override
    public Future<NewMycatConnection> getConnetion(MycatDataContext dataContext,
                                              int order, int refCount, String targetArg, long deadline,
                                              Future<NewMycatConnection> recycleConnectionFuture) {
        synchronized (futures) {
            String target = dataContext.resolveDatasourceTargetName(targetArg, true);
            Future<NewMycatConnection> sqlConnectionFuture = futures.get(target);
            if (sqlConnectionFuture == null) {
                futures.put(target, recycleConnectionFuture);
                XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
                return transactionSession.getConnection(target);
            } else {
                Promise<NewMycatConnection> promise = Promise.promise();
                futures.put(target, recycleConnectionFuture);
                sqlConnectionFuture.onComplete(promise);
                return promise.future();
            }
        }
    }
}

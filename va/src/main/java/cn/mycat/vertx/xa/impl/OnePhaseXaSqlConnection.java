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

import cn.mycat.vertx.xa.MySQLManager;
import cn.mycat.vertx.xa.State;
import cn.mycat.vertx.xa.XaLog;
import io.mycat.newquery.NewMycatConnection;
import io.vertx.core.Future;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.SqlConnection;

import java.util.function.Supplier;

public class OnePhaseXaSqlConnection extends BaseXaSqlConnection {
    private static final Logger LOGGER = LoggerFactory.getLogger(OnePhaseXaSqlConnection.class);

    public OnePhaseXaSqlConnection(Supplier<MySQLManager> mySQLManagerSupplier, XaLog xaLog) {
        super(mySQLManagerSupplier, xaLog);
    }

    @Override
    public Future<Void> commit() {
        return Future.future(promise -> {
            if (map.size() == 1) {
                NewMycatConnection sqlConnection = map.values().iterator().next();
                Future<Void> xaEnd = executeTranscationConnection(connection ->
                        connection.update(String.format(XA_END, xid)).mapEmpty());
                xaEnd.onFailure(promise::fail);
                xaEnd.onSuccess(event -> {
                    changeTo(sqlConnection, State.XA_ENDED);
                    executeTranscationConnection(connection -> connection.update(String.format(XA_COMMIT_ONE_PHASE, xid)).mapEmpty()).onComplete(event1 -> {
                        if (event1.succeeded()) {
                            changeTo(sqlConnection, State.XA_COMMITED);
                            inTranscation = false;
                        }
                        promise.handle(event1);
                    });
                });
            } else {
                super.commit().onComplete(promise);
            }
        });
    }
}

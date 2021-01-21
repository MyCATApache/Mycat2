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

package java.cn.mycat.vertx.xa;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.sqlclient.SqlConnection;

import java.util.function.Supplier;

public interface XaSqlConnection {
    public static String XA_START = "XA START '%s';";
    public static String XA_END = "XA END '%s';";
    public static String XA_COMMIT = "XA COMMIT '%s';";
    public static String XA_PREPARE = "XA PREPARE '%s';";
    public static String XA_ROLLBACK = "XA ROLLBACK '%s';";
    public static String XA_COMMIT_ONE_PHASE = "XA COMMIT '%s' ONE PHASE;";
    public static String XA_RECOVER = "XA RECOVER;";


    public void begin(Handler<AsyncResult<Void>> handler);


    public Future<SqlConnection> getConnection(String targetName);

    public void rollback(Handler<AsyncResult<Void>> handler);

    public void commit(Handler<AsyncResult<Void>> handler);

    /**inner interface
     * @param beforeCommit for the native connection commit or some exception test
     * @param handler the callback handler
     */
    public void commitXa(Supplier<Future> beforeCommit, Handler<AsyncResult<Void>> handler);

    public default void close() {
        close(event -> {

        });
    }

    public void close(Handler<AsyncResult<Void>> handler);

    /**
     * a sql runs before call it;
     * @param handler the callbackhandler
     */
    public void openStatementState(Handler<AsyncResult<Void>> handler);

    /**
     * a sql runs after call it;
     * @param handler the callback handler
     */
    public void closeStatementState(Handler<AsyncResult<Void>> handler);

    public void setAutocommit(boolean b);

    public boolean isAutocommit();

    public boolean isInTranscation();

}

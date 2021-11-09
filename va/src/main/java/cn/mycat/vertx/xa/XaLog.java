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

import java.io.Closeable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

public interface XaLog extends AutoCloseable, Closeable {

    /**
     *
     * @return xid
     */
    String nextXid();

    /**
     *
     * @return transcation timeout;
     */
    long getTimeout();

    /**
     *
     * @return getTimeout+now;
     */
    long getExpires();

    /**
     *
     * @return time of rollback or commit retry delay
     */
    long retryDelay();


    void beginXa(String xid);

    void log(String xid, ImmutableParticipantLog[] participantLogs);

    void log(String xid, String target, State state) ;

    void logRollback(String xid, boolean succeed);

    /**
     * all participants ared prepared.only for check.
     * @param xid xid
     * @param succeed the prepare is successful or not
     */
    void logPrepare(String xid, boolean succeed);

    /**
     * all participants are commited.only for check.
     * @param xid xid
     * @param succeed the commit is successful or not
     */
    void logCommit(String xid, boolean succeed);

    /**
     * Need distributed order, persistence.for recover.
     * @param xid xid
     * @return
     */
    ImmutableCoordinatorLog logCommitBeforeXaCommit(String xid) throws Exception;

    /**
     * Need distributed order, persistence.for recover.
     * @param xid xid
     */
    void logCancelCommitBeforeXaCommit(String xid);

    public void readXARecoveryLog();

    public void readXARecoveryLog(Map<String, Connection> connectionMap) throws SQLException;
}

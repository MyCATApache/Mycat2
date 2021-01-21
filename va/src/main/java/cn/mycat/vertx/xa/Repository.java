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

import java.util.Collection;
import java.util.concurrent.TimeUnit;

public interface Repository {

    /**
     *
     * @return transcation timeout config;
     */
    default long getTimeout() {
        return TimeUnit.SECONDS.toMillis(30);
    }

    default long retryDelayTime() {
        return TimeUnit.SECONDS.toMillis(3);
    }

    void init();

    /**
     * save log
     * @param xid xid
     * @param coordinatorLog CoordinatorLog
     */
    void put(String xid, ImmutableCoordinatorLog coordinatorLog);

    void remove(String xid);

    ImmutableCoordinatorLog get(String xid);

    /**
     *
     * @return get all saved log
     */
    Collection<ImmutableCoordinatorLog> getCoordinatorLogs();

    void close();

    /**
     *  Atomic, persistent ,write the Confirm ready to commit flag log
     * @param coordinatorLog coordinatorLog
     */
    default void writeCommitLog(ImmutableCoordinatorLog coordinatorLog) {
        put(coordinatorLog.getXid(), coordinatorLog);
    }

    /**
     *  Atomic, persistent ,write the cancel the commit flag log
     * @param xid xid
     */
    default void cancelCommitLog(String xid) {
        ImmutableCoordinatorLog immutableCoordinatorLog = get(xid);
        immutableCoordinatorLog.withCommit(false);
        put(xid, immutableCoordinatorLog);
    }
}

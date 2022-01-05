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

import cn.mycat.vertx.xa.*;
import com.alibaba.druid.util.JdbcUtils;
import io.mycat.newquery.RowSet;
import io.vertx.core.*;
import io.vertx.core.impl.ConcurrentHashSet;
import io.vertx.core.impl.logging.Logger;
import io.vertx.core.impl.logging.LoggerFactory;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.SqlConnection;
import lombok.SneakyThrows;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class XaLogImpl implements XaLog {
    private final static Logger LOGGER = LoggerFactory.getLogger(XaLogImpl.class);
    private final Repository xaRepository;
    private final AtomicLong xaIdSeq;
    private final MySQLManager mySQLManager;

    public static XaLogImpl createXaLogImpl(Repository xaRepository, MySQLManager mySQLManager) {
        XaLogImpl xaLog = new XaLogImpl(xaRepository, 0, mySQLManager);
        return xaLog;
    }


    public XaLogImpl(Repository xaRepository, long workerId, MySQLManager mySQLManager) {
        this.mySQLManager = mySQLManager;
        this.xaRepository = xaRepository;
        this.xaIdSeq = new AtomicLong((System.currentTimeMillis() >> 32) + (workerId << 32));

    }

    public static XaLog createXaLog(MySQLManager mySQLManager) {
        return new XaLogImpl(new MemoryRepositoryImpl(), 0, mySQLManager);
    }

//    private void commit(Promise<Object> res, ImmutableCoordinatorLog entry) {
//        List<Future> list = new ArrayList<>();
//        for (ImmutableParticipantLog participant : entry.getParticipants()) {
//            if (participant.getState() == State.XA_PREPARED) {
//                list.add(mySQLManager.getConnection(participant.getTarget())
//                        .compose(sqlConnection -> {
//                            Future<SqlConnection> future = Future.succeededFuture(sqlConnection);
//                            return future
//                                    .compose(connection -> connection.query(
//                                            String.format(XaSqlConnection.XA_COMMIT, entry.getXid())
//
//                                    ).execute().map(c -> {
//                                        Future<Void> closeFuture = sqlConnection.close();
//                                        try {
//                                            log(entry.getXid(), participant.getTarget(), State.XA_COMMITED);
//                                            checkState(entry.getXid(), true, State.XA_COMMITED);
//                                        } catch (Exception e) {
//                                            return CompositeFuture.join(closeFuture, Future.failedFuture(e));
//                                        }
//                                        return closeFuture;
//                                    }));
//                        }));
//            } else {
//                list.add(Future.succeededFuture());
//            }
//        }
//        CompositeFuture.join(list).onComplete(unused -> {
//            try {
//                logCommit(entry.getXid(), unused.succeeded());
//            } catch (Exception e) {
//                res.fail(e);
//                return;
//            }
//            res.tryComplete();
//        });
//    }
//
//    private void rollback(Promise<Object> res, ImmutableCoordinatorLog entry) {
//        List<Future> list = new ArrayList<>();
//        for (ImmutableParticipantLog participant : entry.getParticipants()) {
//            if (participant.getState() == State.XA_PREPARED || participant.getState() == State.XA_ENDED) {
//                list.add(mySQLManager.getConnection(participant.getTarget())
//                        .compose(sqlConnection -> {
//                            Future<SqlConnection> future = Future.succeededFuture(sqlConnection);
//                            return future
//                                    .compose(connection -> connection.query(
//                                            String.format(XaSqlConnection.XA_ROLLBACK, entry.getXid())
//
//                                    ).execute().map(c -> {
//                                        Future<Void> closeFuture = sqlConnection.close();
//                                        try {
//                                            log(entry.getXid(), participant.getTarget(), State.XA_ROLLBACKED);
//                                            checkState(entry.getXid(), true, State.XA_ROLLBACKED);
//                                        } catch (Exception e) {
//                                            return CompositeFuture.join(closeFuture, Future.failedFuture(e));
//                                        }
//                                        return closeFuture;
//                                    }));
//                        }));
//            } else {
//                list.add(Future.succeededFuture());
//            }
//            CompositeFuture.join(list).onComplete(unused -> {
//                try {
//                    logRollback(entry.getXid(), unused.succeeded());
//                    res.tryComplete();
//                } catch (Exception e) {
//                    res.tryFail(e);
//                }
//
//            });
//        }
//    }

    @SneakyThrows
    public void readXARecoveryLog() {
        Map<String, Connection> connectionMap = mySQLManager.getWriteableConnectionMap();
        readXARecoveryLog(connectionMap);
    }

    public void readXARecoveryLog(Map<String, Connection> connectionMap) throws SQLException {
        try {
            try {
                for (Map.Entry<String, Connection> connectionEntry : connectionMap.entrySet()) {
                    Connection connection = connectionEntry.getValue();
                    JdbcUtils.executeUpdate(connection,
                            "create database if not exists `mycat`", Collections.emptyList());
                    JdbcUtils.executeUpdate(connection,
                            "create table if not exists `mycat`." + "`xa_log`"
                                    + "(`xid` bigint PRIMARY KEY NOT NULL" +
                                    ") ENGINE=InnoDB", Collections.emptyList());
                }
            } catch (Throwable throwable) {
                LOGGER.warn("create database mycat fail",throwable);
                return;
            }
            ConcurrentHashMap<String, Set<String>> xid_targets = new ConcurrentHashMap<>();//xid,Se
            for (Map.Entry<String, Connection> connectionEntry : connectionMap.entrySet()) {
                String targetName = connectionEntry.getKey();
                Connection connectionEntryValue = connectionEntry.getValue();
                List<Map<String, Object>> maps = Collections.emptyList();
                try {
                    maps = JdbcUtils.executeQuery(connectionEntryValue, "XA RECOVER", Collections.emptyList());
                } catch (Throwable throwable) {
                    LOGGER.warn(throwable);
                }
                for (Map<String, Object> map : maps) {
                    Set<String> targetNameSet = xid_targets.computeIfAbsent((String) map.get("data"), (s) -> new ConcurrentHashSet<>());
                    targetNameSet.add(targetName);
                }
            }
            Set<String> xidSet = new ConcurrentHashSet<>();//xid set
            for (Map.Entry<String, Connection> connectionEntry : connectionMap.entrySet()) {
                Connection connectionEntryValue = connectionEntry.getValue();
                List<Map<String, Object>> maps = JdbcUtils.executeQuery(connectionEntryValue, "select xid from mycat.xa_log", Collections.emptyList());
                for (Map<String, Object> map : maps) {
                    xidSet.add((String) Objects.toString(map.get("xid")));
                }
            }
            for (Map.Entry<String, Set<String>> entry : xid_targets.entrySet()) {
                String xid = entry.getKey();
                Set<String> targets = entry.getValue();
                String sql;
                if (xidSet.contains(xid)) {
                    sql = "XA COMMIT '" + xid + "'";
                } else {
                    sql = "XA ROLLBACK '" + xid + "'";
                }
                for (String target : targets) {
                    Connection connection = connectionMap.get(target);
                    try {
                        JdbcUtils.executeUpdate(connection, sql, Collections.emptyList());
                    } catch (Exception e) {
                        LOGGER.error(e);//已经提交或者回滚了
                    }
                    JdbcUtils.executeUpdate(connection, "delete from mycat.xa_log where xid = '" + xid + "'", Collections.emptyList());
                }
            }
        } finally {
            connectionMap.forEach((k, v) -> JdbcUtils.close(v));
        }
    }


    @Override
    public String nextXid() {
        long seq = xaIdSeq.getAndUpdate(operand -> {
            if (operand < 0) {
                return 0;
            }
            return ++operand;
        });
        return String.valueOf(seq);
    }

    @Override
    public long getTimeout() {
        return xaRepository.getTimeout();
    }

    @Override
    public void log(String xid, ImmutableParticipantLog[] participantLogs) {
        if (xid == null) return;
        synchronized (xaRepository) {
            ImmutableCoordinatorLog immutableCoordinatorLog = new ImmutableCoordinatorLog(xid, participantLogs);
            xaRepository.put(xid, immutableCoordinatorLog);
        }
    }

    @Override
    public void log(String xid, String target, State state) {
        if (xid == null) return;
        synchronized (xaRepository) {
            ImmutableCoordinatorLog coordinatorLog = xaRepository.get(xid);
            if (coordinatorLog == null) {
                log(xid, new ImmutableParticipantLog[]{new ImmutableParticipantLog(target,
                        getExpires(), state)});
            } else {
                boolean hasCommited = coordinatorLog.mayContains(State.XA_COMMITED);
                ImmutableParticipantLog[] logs = coordinatorLog.replace(target, state, getExpires());
                if (state == State.XA_COMMITED && !hasCommited) {
                    log(xid, logs);
                } else {
                    log(xid, logs);
                }
            }
        }
    }

    @Override
    public void logRollback(String xid, boolean succeed) {
        if (xid == null) return;
        //only log
        if (!checkState(xid, succeed, State.XA_ROLLBACKED)) {
            LOGGER.error("check logRollback xid:" + xid + " error");
        } else if (succeed) {
            synchronized (xaRepository) {
                xaRepository.remove(xid);
            }
        }
    }

    @Override
    public void logPrepare(String xid, boolean succeed) {
        if (xid == null) return;
        //only log
        if (!checkState(xid, succeed, State.XA_PREPARED)) {
            LOGGER.error("check logPrepare xid:" + xid + " error");
        }
    }

    private boolean checkState(String xid, boolean succeed, State state) {
        boolean s;
        ImmutableCoordinatorLog coordinatorLog = xaRepository.get(xid);

        if (coordinatorLog != null) {
            State recordState = coordinatorLog.computeMinState();
            s = (succeed == (recordState == state)) || (succeed && (recordState == State.XA_COMMITED || recordState == State.XA_ROLLBACKED));
        } else {
            s = true;
        }
        return s;
    }

    @Override
    public void logCommit(String xid, boolean succeed) {
        if (xid == null) return;
        //only log
        if (!checkState(xid, succeed, State.XA_COMMITED)) {
            LOGGER.error("check logCommit xid:" + xid + " error");
        } else if (succeed) {
            synchronized (xaRepository) {
                xaRepository.remove(xid);
            }
        }
    }

    @Override
    public ImmutableCoordinatorLog logCommitBeforeXaCommit(String xid) {
        if (xid == null) return null;
        //only log

        synchronized (xaRepository) {
            ImmutableCoordinatorLog immutableCoordinatorLog = xaRepository.get(xid);
            immutableCoordinatorLog.withCommit(true);
            xaRepository.writeCommitLog(immutableCoordinatorLog);
            return immutableCoordinatorLog;
        }
    }


    @Override
    public void logCancelCommitBeforeXaCommit(String xid) {
        if (xid == null) return;
        //only log
        synchronized (xaRepository) {
            xaRepository.cancelCommitLog(xid);
        }
    }

    @Override
    public void beginXa(String xid) {
        if (xid == null) return;
        //only log
    }

    @Override
    public long getExpires() {
        return getTimeout() + System.currentTimeMillis();
    }

    @Override
    public long retryDelay() {
        return xaRepository.retryDelayTime();
    }

    @Override
    public void close() throws IOException {
        xaRepository.close();
        mySQLManager.close();
    }
}

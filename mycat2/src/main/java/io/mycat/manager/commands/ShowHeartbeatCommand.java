//package io.mycat.manager.commands;
//
//import io.mycat.MycatConfig;
//import io.mycat.MycatDataContext;
//import io.mycat.RootHelper;
//import io.mycat.beans.mycat.ResultSetBuilder;
//import io.mycat.client.MycatRequest;
//import io.mycat.commands.MycatCommand;
//import io.mycat.config.DatasourceRootConfig;
//import io.mycat.replica.PhysicsInstance;
//import io.mycat.replica.ReplicaSelectorRuntime;
//import io.mycat.replica.heartbeat.DatasourceStatus;
//import io.mycat.replica.heartbeat.HeartBeatStatus;
//import io.mycat.replica.heartbeat.HeartbeatFlow;
//import io.mycat.util.Response;
//import org.jetbrains.annotations.NotNull;
//
//import java.sql.JDBCType;
//import java.sql.Timestamp;
//import java.util.*;
//import java.util.stream.Collectors;
//
//public class ShowHeartbeatCommand implements ManageCommand {
//    @Override
//    public String statement() {
//        return "show @@backend.heartbeat";
//    }
//
//    @Override
//    public String description() {
//        return "show @@backend.heartbeat";
//    }
//
//    @Override
//    public void handle(MycatRequest request, MycatDataContext context, Response response) {
//
//        ResultSetBuilder resultSetBuilder = getResultSet();
//        response.sendResultSet(()->resultSetBuilder.build());
//    }
//    public static ResultSetBuilder getResultSet() {
//        MycatConfig mycatConfig = RootHelper.INSTANCE.getConfigProvider().currentConfig();
//        Map<String, DatasourceRootConfig.DatasourceConfig> dataSourceConfig = mycatConfig.getDatasource().getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));
//
//        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
//
//        resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);
//        resultSetBuilder.addColumnInfo("TYPE", JDBCType.VARCHAR);
//        resultSetBuilder.addColumnInfo("READABLE", JDBCType.BOOLEAN);
//        resultSetBuilder.addColumnInfo("SESSION_COUNT", JDBCType.BIGINT);
//        resultSetBuilder.addColumnInfo("WEIGHT", JDBCType.BIGINT);
//        resultSetBuilder.addColumnInfo("ALIVE", JDBCType.BOOLEAN);
//        resultSetBuilder.addColumnInfo("MASTER", JDBCType.BOOLEAN);
//        resultSetBuilder.addColumnInfo("HOST", JDBCType.VARCHAR);
//        resultSetBuilder.addColumnInfo("PORT", JDBCType.BIGINT);
//        resultSetBuilder.addColumnInfo("LIMIT_SESSION_COUNT", JDBCType.BIGINT);
//        resultSetBuilder.addColumnInfo("REPLICA", JDBCType.VARCHAR);
//        resultSetBuilder.addColumnInfo("SLAVE_THRESHOLD", JDBCType.BIGINT);
//        resultSetBuilder.addColumnInfo("IS_HEARTBEAT_TIMEOUT", JDBCType.BOOLEAN);
//        resultSetBuilder.addColumnInfo("HB_ERROR_COUNT", JDBCType.BIGINT);
//        resultSetBuilder.addColumnInfo("HB_LAST_SWITCH_TIME", JDBCType.DATE);
//        resultSetBuilder.addColumnInfo("HB_MAX_RETRY", JDBCType.BIGINT);
//        resultSetBuilder.addColumnInfo("IS_CHECKING", JDBCType.BOOLEAN);
//        resultSetBuilder.addColumnInfo("MIN_SWITCH_TIME_INTERVAL", JDBCType.BIGINT);
//        resultSetBuilder.addColumnInfo("HEARTBEAT_TIMEOUT", JDBCType.BIGINT);
//        resultSetBuilder.addColumnInfo("SYNC_DS_STATUS", JDBCType.VARCHAR);
//        resultSetBuilder.addColumnInfo("HB_DS_STATUS", JDBCType.VARCHAR);
//        resultSetBuilder.addColumnInfo("IS_SLAVE_BEHIND_MASTER", JDBCType.BOOLEAN);
//        resultSetBuilder.addColumnInfo("LAST_SEND_QUERY_TIME", JDBCType.DATE);
//        resultSetBuilder.addColumnInfo("LAST_RECEIVED_QUERY_TIME", JDBCType.DATE);
//
//
//        for (HeartbeatFlow heartbeatFlow : ReplicaSelectorRuntime.INSTANCE.getHeartbeatDetectorMap().values()) {
//            PhysicsInstance instance = heartbeatFlow.instance();
//
//            String NAME = instance.getName();
//            String TYPE = instance.getType().name();
//            boolean READABLE = instance.asSelectRead();
//            int SESSION_COUNT = instance.getSessionCounter();
//            int WEIGHT = instance.getWeight();
//            boolean ALIVE = instance.isAlive();
//            boolean MASTER = instance.isMaster();
//
//            long SLAVE_THRESHOLD = heartbeatFlow.getSlaveThreshold();
//
//            boolean IS_HEARTBEAT_TIMEOUT = heartbeatFlow.isHeartbeatTimeout();
//            final HeartBeatStatus HEART_BEAT_STATUS = heartbeatFlow.getHbStatus();
//            int HB_ERROR_COUNT = HEART_BEAT_STATUS.getErrorCount();
//            long HB_LAST_SWITCH_TIME =  (HEART_BEAT_STATUS.getLastSwitchTime());
//            int HB_MAX_RETRY = HEART_BEAT_STATUS.getMaxRetry();
//            boolean IS_CHECKING = HEART_BEAT_STATUS.isChecking();
//            long MIN_SWITCH_TIME_INTERVAL = HEART_BEAT_STATUS.getMinSwitchTimeInterval();
//            final long HEARTBEAT_TIMEOUT =  (heartbeatFlow.getHeartbeatTimeout());
//            DatasourceStatus DS_STATUS_OBJECT = heartbeatFlow.getDsStatus();
//            String SYNC_DS_STATUS = DS_STATUS_OBJECT.getDbSynStatus().name();
//            String HB_DS_STATUS = DS_STATUS_OBJECT.getStatus().name();
//            boolean IS_SLAVE_BEHIND_MASTER = DS_STATUS_OBJECT.isSlaveBehindMaster();
//            Date LAST_SEND_QUERY_TIME = new Date(heartbeatFlow.getLastSendQryTime());
//            Date LAST_RECEIVED_QUERY_TIME = new Date(heartbeatFlow.getLastReceivedQryTime());
//
//            Optional<DatasourceRootConfig.DatasourceConfig> e = Optional.ofNullable(dataSourceConfig.get(NAME));
//
//            String replicaDataSourceSelectorList =String.join(",", ReplicaSelectorRuntime.INSTANCE.getRepliaNameListByInstanceName(NAME));
//
//            resultSetBuilder.addObjectRowPayload(
//                    Arrays.asList(NAME,
//                            TYPE,
//                            READABLE,
//                            SESSION_COUNT,
//                            WEIGHT,
//                            ALIVE,
//                            MASTER,
//                            e.map(i -> i.getIp()).orElse(""),
//                            e.map(i -> i.getPort()).orElse(-1),
//                            e.map(i -> i.getMaxCon()).orElse(-1),
//                            replicaDataSourceSelectorList,
//                            SLAVE_THRESHOLD,
//                            IS_HEARTBEAT_TIMEOUT,
//                            HB_ERROR_COUNT,
//                            HB_LAST_SWITCH_TIME,
//                            HB_MAX_RETRY,
//                            IS_CHECKING,
//                            MIN_SWITCH_TIME_INTERVAL,
//                            HEARTBEAT_TIMEOUT,
//                            SYNC_DS_STATUS,
//                            HB_DS_STATUS,
//                            IS_SLAVE_BEHIND_MASTER,
//                            LAST_SEND_QUERY_TIME,
//                            LAST_RECEIVED_QUERY_TIME
//                    ));
//        }
//        return resultSetBuilder;
//    }
//}
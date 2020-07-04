package io.mycat.manager.commands;

import io.mycat.MycatConfig;
import io.mycat.MycatDataContext;
import io.mycat.RootHelper;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.config.ClusterRootConfig;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaDataSourceSelector;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.replica.ReplicaSwitchType;
import io.mycat.util.Response;
import org.jetbrains.annotations.NotNull;

import java.sql.JDBCType;
import java.util.*;
import java.util.stream.Collectors;

public class ShowReplicaCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@backend.replica";
    }

    @Override
    public String description() {
        return "show @@backend.replica";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        ResultSetBuilder resultSetBuilder = getResultSet();
        response.sendResultSet(() -> resultSetBuilder.build());
    }

    @NotNull
    public static ResultSetBuilder getResultSet() {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("SWITCH_TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("MAX_REQUEST_COUNT", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("WRITE_DS", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("READ_DS", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("WRITE_L", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("READ_L", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("AVAILABLE", JDBCType.BOOLEAN);
        Collection<ReplicaDataSourceSelector> values =
                ReplicaSelectorRuntime.INSTANCE.getReplicaMap().values();
        MycatConfig mycatConfig = RootHelper.INSTANCE.getConfigProvider().currentConfig();

        Map<String, ClusterRootConfig.ClusterConfig> clusterConfigMap = mycatConfig.getCluster().getClusters().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));

        for (ReplicaDataSourceSelector value : values) {
            String NAME = value.getName();

            Optional<ClusterRootConfig.ClusterConfig> e = Optional.ofNullable(clusterConfigMap.get(NAME));

            ReplicaSwitchType SWITCH_TYPE = value.getSwitchType();
            int MAX_REQUEST_COUNT = value.maxRequestCount();
            String TYPE = value.getBalanceType().name();
            String WRITE_DS = ((List<PhysicsInstance>) value.getWriteDataSource()).stream().map(i -> i.getName()).collect(Collectors.joining(","));
            String READ_DS = (value.getReadDataSource()).stream().map(i -> i.getName()).collect(Collectors.joining(","));
            String WL = Optional.ofNullable(value.getDefaultWriteLoadBalanceStrategy()).map(i -> i.getClass().getName()).orElse(null);
            String RL = Optional.ofNullable(value.getDefaultReadLoadBalanceStrategy()).map(i -> i.getClass().getName()).orElse(null);
            boolean AVAILABLE = ((List<PhysicsInstance>) value.getWriteDataSource()).stream().anyMatch(PhysicsInstance::isAlive);

            resultSetBuilder.addObjectRowPayload(
                    Arrays.asList(NAME, SWITCH_TYPE, MAX_REQUEST_COUNT, TYPE,
                            WRITE_DS, READ_DS,
                            WL, RL,AVAILABLE
                    ));
        }
        return resultSetBuilder;
    }

}
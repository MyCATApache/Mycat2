package io.mycat.manager.commands;

import io.mycat.MycatConfig;
import io.mycat.MycatDataContext;
import io.mycat.RootHelper;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaDataSourceSelector;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.*;
import java.util.stream.Collectors;

public class ShowInstanceCommand implements ManageCommand {
    @Override
    public String statement() {
        return "show @@backend.instance";
    }

    @Override
    public String description() {
        return "show @@backend.instance(proxy or jdbc ref to instance)";
    }

    @Override
    public void handle(MycatRequest request, MycatDataContext context, Response response) {
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
        resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);

        resultSetBuilder.addColumnInfo("ALIVE", JDBCType.BOOLEAN);
        resultSetBuilder.addColumnInfo("READABLE", JDBCType.BOOLEAN);
        resultSetBuilder.addColumnInfo("TYPE", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("SESSION_COUNT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("WEIGHT", JDBCType.BIGINT);

        resultSetBuilder.addColumnInfo("MASTER", JDBCType.BOOLEAN);
        resultSetBuilder.addColumnInfo("HOST", JDBCType.VARCHAR);
        resultSetBuilder.addColumnInfo("PORT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("LIMIT_SESSION_COUNT", JDBCType.BIGINT);
        resultSetBuilder.addColumnInfo("REPLICA", JDBCType.VARCHAR);
        Collection<PhysicsInstance> values =
                ReplicaSelectorRuntime.INSTANCE.getPhysicsInstanceMap().values();
        MycatConfig mycatConfig = RootHelper.INSTANCE.getConfigProvider().currentConfig();
        Map<String, DatasourceRootConfig.DatasourceConfig> dataSourceConfig = mycatConfig.getDatasource().getDatasources().stream().collect(Collectors.toMap(k -> k.getName(), v -> v));


        for (PhysicsInstance instance : values) {

            String NAME = instance.getName();
            String TYPE = instance.getType().name();
            boolean READABLE = instance.asSelectRead();
            int SESSION_COUNT = instance.getSessionCounter();
            int WEIGHT = instance.getWeight();
            boolean ALIVE = instance.isAlive();
            boolean MASTER = instance.isMaster();

            Optional<DatasourceRootConfig.DatasourceConfig> e = Optional.ofNullable(dataSourceConfig.get(NAME));


            String replicaDataSourceSelectorList =String.join(",", ReplicaSelectorRuntime.INSTANCE.getRepliaNameListByInstanceName(NAME));

            resultSetBuilder.addObjectRowPayload(
                    Arrays.asList(NAME, ALIVE, READABLE,TYPE, SESSION_COUNT, WEIGHT, MASTER,
                            e.map(i -> i.getIp()).orElse(""),
                            e.map(i -> i.getPort()).orElse(-1),
                            e.map(i -> i.getMaxCon()).orElse(-1),
                            replicaDataSourceSelectorList
                    ));
        }
        response.sendResultSet(() -> resultSetBuilder.build());
    }
}
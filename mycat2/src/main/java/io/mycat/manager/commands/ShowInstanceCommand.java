package io.mycat.manager.commands;

import io.mycat.MycatConfig;
import io.mycat.MycatDataContext;
import io.mycat.RootHelper;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatCommand;
import io.mycat.config.DatasourceRootConfig;
import io.mycat.replica.PhysicsInstance;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.util.Response;

import java.sql.JDBCType;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class ShowInstanceCommand implements MycatCommand {
    @Override
    public boolean run(MycatRequest request, MycatDataContext context, Response response) {
        if ("show @@backend.instance".equalsIgnoreCase(request.getText())){
            ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
            resultSetBuilder.addColumnInfo("NAME", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("TYPE", JDBCType.VARCHAR);
            resultSetBuilder.addColumnInfo("READABLE", JDBCType.BOOLEAN);
            resultSetBuilder.addColumnInfo("SESSION_COUNT", JDBCType.BIGINT);
            resultSetBuilder.addColumnInfo("WEIGHT", JDBCType.BIGINT);
            resultSetBuilder.addColumnInfo("ALIVE", JDBCType.BOOLEAN);
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

                resultSetBuilder.addObjectRowPayload(
                        Arrays.asList(NAME,TYPE, READABLE,SESSION_COUNT,WEIGHT,ALIVE,MASTER,
                                e.map(i->i.getIp()).orElse(""),
                                e.map(i->i.getPort()).orElse(-1),
                                e.map(i->i.getMaxCon()).orElse(-1),
                                ReplicaSelectorRuntime.INSTANCE.getReplicaMap().values()
                                        .stream().flatMap(i->i.getRawDataSourceMap().values().stream())
                                        .filter(i->NAME.equals(i.getName())).findFirst().orElse(null)
                                ));
            }
            response.sendResultSet(() -> resultSetBuilder.build());
            return true;
        }
        return false;
    }

    @Override
    public boolean explain(MycatRequest request, MycatDataContext context, Response response) {
        return false;
    }

    @Override
    public String getName() {
        return getClass().getName();
    }
}
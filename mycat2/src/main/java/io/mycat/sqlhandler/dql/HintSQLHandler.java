package io.mycat.sqlhandler.dql;

import com.alibaba.fastsql.sql.ast.SQLCommentHint;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlHintStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatRouterConfigOps;
import io.mycat.client.MycatRequest;
import io.mycat.commands.MycatdbCommand;
import io.mycat.config.*;
import io.mycat.sqlhandler.*;
import io.mycat.sqlhandler.ddl.CreateTableSQLHandler;
import io.mycat.util.JsonUtil;
import io.mycat.util.Response;

import java.util.List;
import java.util.Map;
import java.util.Optional;

public class HintSQLHandler extends AbstractSQLHandler<MySqlHintStatement> {
    @Override
    protected void onExecute(SQLRequest<MySqlHintStatement> request, MycatDataContext dataContext, Response response) {
        Optional<Map<String, Object>> afterJson = request.getAnyJson();
        MySqlHintStatement ast = request.getAst();
        List<SQLCommentHint> hints = ast.getHints();
        if (hints.size() == 1) {
            String s = SqlHints.unWrapperHint(hints.get(0).getText());
            if (s.startsWith("mycat:")) {
                s = s.substring(6);
                int bodyStartIndex = s.indexOf('{');
                String cmd = s.substring(0, bodyStartIndex);
                String body = s.substring(bodyStartIndex);
                mycatDmlHandler(cmd, body);
            }
            System.out.println();
        }
        response.sendOk();
    }

    public static void mycatDmlHandler(String cmd, String body) {
        if ("createTable".equalsIgnoreCase(cmd)) {
            CreateTableSQLHandler.INSTANCE.createTable(
                    JsonUtil.from(body, Map.class),
                    null,
                    null,
                    null
            );
        }
        if ("dropTable".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                Map map = JsonUtil.from(body, Map.class);
                String schemaName = (String)map.get("schemaName");
                String tableName = (String)map.get("tableName");
                ops.removeTable(schemaName,tableName);
                ops.commit();
            }
        }
        if ("addDatasource".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.putDatasource( JsonUtil.from(body, DatasourceConfig.class));
                ops.commit();
            }
        }
        if ("removeDatasource".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.removeDatasource(JsonUtil.from(body, DatasourceConfig.class).getName());
                ops.commit();
            }
        }
        if ("addUser".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.putUser(JsonUtil.from(body, UserConfig.class));
                ops.commit();
            }
        }
        if ("removeUser".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.deleteUser(JsonUtil.from(body, UserConfig.class).getUsername());
                ops.commit();
            }
        }
        if ("addCluster".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.putReplica(JsonUtil.from(body, ClusterConfig.class));
                ops.commit();
            }
        }
        if ("removeCluster".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.removeReplica(JsonUtil.from(body, ClusterConfig.class).getName());
                ops.commit();
            }
        }
        if ("addSequence".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.putSequence(JsonUtil.from(body, SequenceConfig.class));
                ops.commit();
            }
        }
        if ("removeSequence".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.removeSequence(JsonUtil.from(body, SequenceConfig .class).getName());
                ops.commit();
            }
        }
        if ("putSchema".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.putSchema(JsonUtil.from(body, LogicSchemaConfig.class));
                ops.commit();
            }
        }
        if ("removeSchema".equalsIgnoreCase(cmd)) {
            try (MycatRouterConfigOps ops = ConfigUpdater.getOps()) {
                ops.dropSchema(JsonUtil.from(body,LogicSchemaConfig.class).getSchemaName());
                ops.commit();
            }
        }
    }
}
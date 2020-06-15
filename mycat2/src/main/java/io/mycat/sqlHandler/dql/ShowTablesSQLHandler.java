package io.mycat.sqlHandler.dql;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLName;
import com.alibaba.fastsql.sql.ast.statement.SQLShowTablesStatement;
import io.mycat.DDLManager;
import io.mycat.MycatDataContext;
import io.mycat.api.collector.ComposeRowBaseIterator;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.JdbcRuntime;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.metadata.MetadataManager;
import io.mycat.replica.ReplicaSelectorRuntime;
import io.mycat.router.ShowStatementRewriter;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Optional;


public class ShowTablesSQLHandler extends AbstractSQLHandler<SQLShowTablesStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowTablesSQLHandler.class);

    @Override
    protected ExecuteCode onExecute(SQLRequest<SQLShowTablesStatement> request, MycatDataContext dataContext, Response response) {
        DDLManager.INSTANCE.updateTables();
        String sql = ShowStatementRewriter.rewriteShowTables(dataContext.getDefaultSchema(), request.getAst());
        LOGGER.info(sql);
        //show 语句变成select 语句

        try (RowBaseIterator query = MycatDBs.createClient(dataContext).query(sql)) {
            //schema上默认的targetName;
            try {
                SQLShowTablesStatement showTablesStatement = request.getAst();
                SQLName from = showTablesStatement.getFrom();
                String schema = SQLUtils.normalize(from == null ? dataContext.getDefaultSchema() : from.getSimpleName());
                if (WithDefaultTargetInfo(response, sql, query, schema)) return ExecuteCode.PERFORMED;
            } catch (Exception e) {
                LOGGER.error("", e);
            }
            response.sendResultSet(()->query, null);
            return ExecuteCode.PERFORMED;
        }
    }

    private boolean WithDefaultTargetInfo(Response response, String sql, RowBaseIterator query, String schema) {
        if (schema != null) {
            String defaultTargetName = Optional.ofNullable(MetadataManager.INSTANCE)
                    .map(i -> i.getSchemaMap())
                    .map(i -> i.get(schema))
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    //暂时定为没有配置分片表才读取默认targetName的表作为tables
                    .filter(i -> i.logicTables() == null || i.logicTables().isEmpty())
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
                    .map(i -> i.defaultTargetName())
                    .orElse(null);
            if (defaultTargetName != null) {
                defaultTargetName = ReplicaSelectorRuntime.INSTANCE.getDatasourceNameByReplicaName(defaultTargetName, true, null);
                try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(defaultTargetName)) {
                    RowBaseIterator rowBaseIterator = connection.executeQuery(sql);

                    //safe
                    response.sendResultSet(()-> ComposeRowBaseIterator.of(rowBaseIterator, query), null);
                    return true;
                }
            }
        }
        return false;
    }


}

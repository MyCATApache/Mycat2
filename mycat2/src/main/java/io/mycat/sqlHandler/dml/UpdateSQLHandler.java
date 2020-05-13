package io.mycat.sqlHandler.dml;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.RootHelper;
import io.mycat.metadata.LogicTableType;
import io.mycat.metadata.ParseContext;
import io.mycat.metadata.SchemaHandler;
import io.mycat.metadata.TableHandler;
import io.mycat.sqlHandler.AbstractSQLHandler;
import io.mycat.sqlHandler.ExecuteCode;
import io.mycat.sqlHandler.SQLRequest;
import io.mycat.upondb.MycatDBClientMediator;
import io.mycat.upondb.MycatDBs;
import io.mycat.util.Response;

import javax.annotation.Resource;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Resource
public class UpdateSQLHandler extends AbstractSQLHandler<MySqlUpdateStatement> {

    @Override
    protected ExecuteCode onExecute(SQLRequest<MySqlUpdateStatement> request, MycatDataContext dataContext, Response response) {
        updateHandler(request.getAst(), dataContext, (SQLExprTableSource) request.getAst().getTableSource(), response);
        return ExecuteCode.PERFORMED;
    }

    public static void updateHandler(SQLStatement sql, MycatDataContext dataContext, SQLExprTableSource tableSource, Response receiver) {
        MycatDBClientMediator mycatDBClientMediator = MycatDBs.createClient(dataContext);
        String schemaName = tableSource.getSchema() == null ? dataContext.getDefaultSchema() : tableSource.getSchema();
        String tableName = tableSource.getTableName();
        SchemaHandler schemaHandler;
        Optional<Map<String, SchemaHandler>> handlerMapOptional = Optional.ofNullable(mycatDBClientMediator)
                .map(i -> i.config())
                .map(i -> i.getSchemaMap());
        Optional<String> targetNameOptional = Optional.ofNullable(RootHelper.INSTANCE)
                .map(i -> i.getConfigProvider())
                .map(i -> i.currentConfig())
                .map(i -> i.getMetadata())
                .map(i -> i.getPrototype())
                .map(i -> i.getTargetName());
        if (!handlerMapOptional.isPresent()) {
            if (targetNameOptional.isPresent()) {
                receiver.proxyUpdate(targetNameOptional.get(), Objects.toString(sql));
                return;
            } else {
                receiver.sendError(new MycatException("Unable to route:" + sql));
                return;
            }
        } else {
            Map<String, SchemaHandler> handlerMap = handlerMapOptional.get();
            schemaHandler = Optional.ofNullable(handlerMap.get(schemaName))
                    .orElseGet(() -> handlerMap.get(mycatDBClientMediator.getSchema()));
            if (schemaHandler == null) {
                receiver.sendError(new MycatException("Unable to route:" + sql));
                return;
            }
        }
        String defaultTargetName = schemaHandler.defaultTargetName();
        Map<String, TableHandler> tableMap = schemaHandler.logicTables();
        TableHandler tableHandler = tableMap.get(tableName);
        ///////////////////////////////common///////////////////////////////
        if (tableHandler == null) {
            receiver.proxyUpdate(defaultTargetName, sql.toString());
            return;
        }
        String string = sql.toString();
        if (sql instanceof MySqlInsertStatement) {
            switch (tableHandler.getType()) {
                case SHARDING:
                    receiver.multiInsert(string, tableHandler.insertHandler().apply(new ParseContext(sql.toString())));
                    break;
                case GLOBAL:
                    receiver.multiGlobalInsert(string, tableHandler.insertHandler().apply(new ParseContext(sql.toString())));
                    break;
            }

        } else if (sql instanceof com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement) {
            switch (tableHandler.getType()) {
                case SHARDING:
                    receiver.multiUpdate(string, tableHandler.deleteHandler().apply(new ParseContext(sql.toString())));
                    break;
                case GLOBAL:
                    receiver.multiGlobalUpdate(string, tableHandler.deleteHandler().apply(new ParseContext(sql.toString())));
                    break;
            }

        } else if (sql instanceof com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement) {
            switch (tableHandler.getType()) {
                case SHARDING:
                    receiver.multiUpdate(string, tableHandler.updateHandler().apply(new ParseContext(sql.toString())));
                    break;
                case GLOBAL:
                    receiver.multiGlobalUpdate(string, tableHandler.deleteHandler().apply(new ParseContext(sql.toString())));
                    break;
            }
        } else {
            throw new UnsupportedOperationException("unsupported statement:"+sql);
        }

    }

    @Override
    public ExecuteCode onExplain(SQLRequest<MySqlUpdateStatement> request, MycatDataContext dataContext, Response response) {
        response.setExplainMode(true);
        updateHandler(request.getAst(), dataContext, (SQLExprTableSource) request.getAst().getTableSource(), response);
        return ExecuteCode.PERFORMED;
    }
}

package io.mycat.sqlhandler.dml;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.*;
import io.mycat.hbt3.DrdsConfig;
import io.mycat.hbt3.DrdsConst;
import io.mycat.hbt3.DrdsRunner;
import io.mycat.hbt3.DrdsSql;
import io.mycat.hbt4.*;
import io.mycat.hbt4.executor.TempResultSetFactory;
import io.mycat.hbt4.executor.TempResultSetFactoryImpl;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.SchemaHandler;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.ExecuteCode;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.util.Response;
import lombok.SneakyThrows;

import java.util.*;

public class UpdateSQLHandler extends AbstractSQLHandler<MySqlUpdateStatement> {

    @Override
    protected void onExecute(SQLRequest<MySqlUpdateStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        updateHandler(request.getAst(), dataContext, (SQLExprTableSource) request.getAst().getTableSource(), response);
    }

    @SneakyThrows
    public static void updateHandler(SQLStatement sqlStatement, MycatDataContext dataContext, SQLExprTableSource tableSource, Response receiver) {
        String schemaName = Optional.ofNullable(tableSource.getSchema() == null ? dataContext.getDefaultSchema() : tableSource.getSchema())
                .map(i-> SQLUtils.normalize(i)).orElse(null);
        String tableName = SQLUtils.normalize(tableSource.getTableName());
        SchemaHandler schemaHandler;
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        Optional<Map<String, SchemaHandler>> handlerMapOptional = Optional.ofNullable(metadataManager.getSchemaMap());
        Optional<String> targetNameOptional = Optional.ofNullable(metadataManager.getPrototype());
        if (!handlerMapOptional.isPresent()) {
            if (targetNameOptional.isPresent()) {
                receiver.proxyUpdate(targetNameOptional.get(), Objects.toString(sqlStatement));
                return;
            } else {
                receiver.sendError(new MycatException("Unable to route:" + sqlStatement));
                return;
            }
        } else {
            Map<String, SchemaHandler> handlerMap = handlerMapOptional.get();
            schemaHandler = Optional.ofNullable(handlerMap.get(schemaName))
                    .orElseGet(() -> {
                        if (dataContext.getDefaultSchema() == null) {
                            throw new MycatException("unknown schema:"+schemaName);//可能schemaName有值,但是值名不是配置的名字
                        }
                        return handlerMap.get(dataContext.getDefaultSchema());
                    });
            if (schemaHandler == null) {
                receiver.sendError(new MycatException("Unable to route:" + sqlStatement));
                return;
            }
        }
        String defaultTargetName = schemaHandler.defaultTargetName();
        Map<String, TableHandler> tableMap = schemaHandler.logicTables();
        TableHandler tableHandler = tableMap.get(tableName);
        ///////////////////////////////common///////////////////////////////
        if (tableHandler == null) {
            receiver.proxyUpdate(defaultTargetName, sqlStatement.toString());
            return;
        }
        TempResultSetFactory tempResultSetFactory = new TempResultSetFactoryImpl();
        DatasourceFactory datasourceFactory = new DefaultDatasourceFactory(dataContext);
        DrdsRunners.runOnDrds(dataContext, sqlStatement,new ResponseExecutorImplementor(datasourceFactory,tempResultSetFactory,receiver));
    }
}

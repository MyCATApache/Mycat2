package io.mycat.sqlHandler.dml;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import io.mycat.ExplainResponse;
import io.mycat.MycatDataContext;
import io.mycat.MycatException;
import io.mycat.RootHelper;
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
        updateHandler(request.getAst(),dataContext,(SQLExprTableSource) request.getAst().getTableSource(),response);
        return ExecuteCode.PERFORMED;
    }
    public static void updateHandler(SQLStatement sql, MycatDataContext dataContext, SQLExprTableSource tableSource, Response receiver) {
        MycatDBClientMediator mycatDBClientMediator = MycatDBs.createClient(dataContext);
        String schemaName = tableSource.getSchema();
        String tableName = tableSource.getTableName();

        ///////////////////////////////common///////////////////////////////
        Map<String, SchemaHandler> schemaMap = mycatDBClientMediator.config().getSchemaMap();
        SchemaHandler schemaHandler = schemaMap.get(schemaName);
        if (schemaHandler == null) {
            String defaultSchema = mycatDBClientMediator.getSchema();
            if (defaultSchema != null) {
                schemaHandler = schemaMap.get(defaultSchema);
            } else {
                Optional<String> targetNameOptional = Optional.ofNullable(RootHelper.INSTANCE)
                        .map(i -> i.getConfigProvider())
                        .map(i -> i.currentConfig())
                        .map(i->i.getMetadata())
                        .map(i->i.getPrototype())
                        .map(i->i.getTargetName());
                if (!targetNameOptional.isPresent()) {
                    receiver.sendError(new MycatException("unknown schema"));
                    return;
                }else {
                    receiver.proxyUpdate(targetNameOptional.get(), Objects.toString(sql));
                    return;
                }
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

        if (sql instanceof MySqlInsertStatement){
            receiver.multiInsert(string,tableHandler.insertHandler().apply(new ParseContext(sql.toString())));
        }else {
            receiver.multiUpdate(string,tableHandler.insertHandler().apply(new ParseContext(sql.toString())));
        }

    }

    @Override
    public ExecuteCode explain(SQLRequest<MySqlUpdateStatement> request, MycatDataContext dataContext, Response response) {
        updateHandler(request.getAst(), dataContext, (SQLExprTableSource) request.getAst().getTableSource(),new ExplainResponse(UpdateSQLHandler.class,response) );
        return ExecuteCode.PERFORMED;
    }
}

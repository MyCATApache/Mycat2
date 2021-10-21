package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLName;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLCallStatement;
import io.mycat.*;
import io.mycat.config.NormalBackEndProcedureInfoConfig;
import io.mycat.config.NormalProcedureConfig;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class SQLCallStatementHandler extends AbstractSQLHandler<SQLCallStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(SQLCallStatementHandler.class);

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLCallStatement> request, MycatDataContext dataContext, Response response) {
        SQLCallStatement ast = request.getAst();
        SQLName procedureExpr = ast.getProcedureName();
        if (procedureExpr instanceof SQLIdentifierExpr && dataContext.getDefaultSchema() != null) {
            String name = ((SQLIdentifierExpr) procedureExpr).getName();
            procedureExpr = new SQLPropertyExpr(new SQLIdentifierExpr(dataContext.getDefaultSchema()), name);
        }
        SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) procedureExpr;
        String schemaName = SQLUtils.normalize(sqlPropertyExpr.getOwnerName(), true);
        String pName = SQLUtils.normalize(sqlPropertyExpr.getName(), true);

        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        Optional<ProcedureHandler> procedureHandlerOptional = metadataManager.getProcedure(schemaName, pName);
        if (procedureHandlerOptional.isPresent()) {
            ProcedureHandler procedureHandler = procedureHandlerOptional.get();
            switch (procedureHandler.getType()) {
                case NORMAL:
                    NormalProcedureHandler normalProcedureHandler = (NormalProcedureHandler) procedureHandler;
                    NormalProcedureConfig config = normalProcedureHandler.getConfig();
                    NormalBackEndProcedureInfoConfig locality = config.getLocality();
                    String targetName = dataContext.resolveDatasourceTargetName(locality.getTargetName(), true);
                    return response.proxyProcedure(ast.toString(), targetName);
            }
            LOGGER.error("unknown call {}", ast);
        }
        return response.proxyProcedure(ast.toString(),MetadataManager.getPrototype());
    }
}

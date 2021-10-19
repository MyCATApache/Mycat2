package io.mycat.sqlhandler.ddl;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLParameter;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLCreateProcedureStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.TableCollector;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.vertx.core.Future;

import java.util.*;
import java.util.stream.Collectors;

public class SQLCreateProcedureHandler extends AbstractSQLHandler<SQLCreateProcedureStatement> {

    Map<String, SQLCreateProcedureStatement> map = new HashMap();

    @Override
    protected Future<Void> onExecute(SQLRequest<SQLCreateProcedureStatement> request, MycatDataContext dataContext, Response response) {
        SQLCreateProcedureStatement ast = request.getAst();
        if (ast.getName() instanceof SQLIdentifierExpr) {
            String defaultSchema = dataContext.getDefaultSchema();
            if (defaultSchema != null) {
                ast.setName(new SQLPropertyExpr("`" + defaultSchema + "`", ((SQLIdentifierExpr) ast.getName()).getName()));
            }
        }

        if (!(ast.getName() instanceof SQLPropertyExpr)) {
            throw new IllegalArgumentException("unknown schema:");
        }
        SQLPropertyExpr pNameExpr = (SQLPropertyExpr) ast.getName();
        String schemaName = SQLUtils.normalize(pNameExpr.getOwnerName().toLowerCase());
        String pName = SQLUtils.normalize(pNameExpr.getName().toLowerCase());
        List<SQLParameter> sqlParameters = Optional.ofNullable(ast.getParameters()).orElse(Collections.emptyList());
        Map<SQLParameter.ParameterType, List<SQLParameter>> parameterTypeListMap
                = sqlParameters.stream().collect(Collectors.groupingBy(k -> k.getParamType()));
        SQLStatement block = ast.getBlock();
        if (dataContext.getDefaultSchema() != null) {
            block.accept(new MySqlASTVisitorAdapter() {
                @Override
                public void endVisit(SQLExprTableSource x) {
                    resolveSQLExprTableSource(x, dataContext);
                }
            });
        }
        Map<String, Collection<String>> collect = TableCollector.collect(dataContext.getDefaultSchema(), block);
        map.put(schemaName+"."+pName,ast);

        return response.proxyUpdateToPrototype(ast.toString());
    }
}

package io.mycat.sqlhandler;

import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlShowRelayLogEventsStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlSchemaStatVisitor;
import io.mycat.DataNode;
import io.mycat.MetaClusterCurrent;
import io.mycat.MycatDataContext;
import io.mycat.hbt4.DataSourceFactory;
import io.mycat.hbt4.DefaultDatasourceFactory;
import io.mycat.hbt4.ResponseExecutorImplementor;
import io.mycat.metadata.MetadataManager;
import io.mycat.sqlhandler.dml.DrdsRunners;
import io.mycat.util.NameMap;
import io.mycat.util.Response;

import java.util.HashSet;
import java.util.Set;

public class ShardingSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    @Override
    protected void onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        try (DataSourceFactory datasourceFactory = new DefaultDatasourceFactory(dataContext)) {
            ResponseExecutorImplementor responseExecutorImplementor = ResponseExecutorImplementor.create(dataContext, response, datasourceFactory);
            SQLSelectStatement selectStatement = request.getAst();
            Set<String> tableNames = new HashSet<>();
            selectStatement.accept(new MySqlASTVisitorAdapter() {
                @Override
                public boolean visit(SQLExprTableSource x) {
                    String tableName = x.getTableName();
                    if (tableName != null) {
                        tableNames.add(SQLUtils.normalize(tableName));
                    }
                    return super.visit(x);
                }
            });
            MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
            if (metadataManager.checkVaildNormalRoute(tableNames)) {
                NameMap<DataNode> normalTables = metadataManager.getNormalTables();
                String[] targetName = new String[1];
                selectStatement.accept(new MySqlASTVisitorAdapter() {
                    @Override
                    public boolean visit(SQLExprTableSource x) {
                        String tableName = x.getTableName();
                        if (tableName != null) {
                            tableName = SQLUtils.normalize(tableName);
                            DataNode dataNode = normalTables.get(tableName, false);
                            x.setSimpleName(dataNode.getTable());
                            x.setSchema(dataNode.getSchema());
                            targetName[0] = dataNode.getTargetName();
                        }
                        return super.visit(x);
                    }
                });
                response.proxySelect(targetName[0],selectStatement.toString());
            } else {
                DrdsRunners.runOnDrds(dataContext, selectStatement, responseExecutorImplementor);
            }
        }
    }
}
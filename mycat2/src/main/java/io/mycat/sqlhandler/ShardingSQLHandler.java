package io.mycat.sqlhandler;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.*;
import io.mycat.util.NameMap;
import io.mycat.util.Pair;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;

public class ShardingSQLHandler extends AbstractSQLHandler<SQLSelectStatement> {
    @Override
    protected void onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) throws Exception {
        SQLSelectStatement selectStatement = request.getAst();
        Set<Pair<String, String>> tableNames = new HashSet<>();
        selectStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public boolean visit(SQLExprTableSource x) {
                String tableName = x.getTableName();
                if (tableName != null) {
                    String schema = Optional.ofNullable(x.getSchema()).orElse(dataContext.getDefaultSchema());
                    if (schema == null) {
                        throw new MycatException("please use schema;");
                    }
                    tableNames.add(Pair.of(schema, SQLUtils.normalize(tableName)));
                }
                return super.visit(x);
            }
        });
        MetadataManager metadataManager = MetaClusterCurrent.wrapper(MetadataManager.class);
        NameMap<MetadataManager.SimpleRoute> normalTables = new NameMap<>();
        boolean singleTarget = metadataManager.checkVaildNormalRoute(tableNames, normalTables);
        if (singleTarget) {
            String[] targetName = new String[1];
            selectStatement.accept(new MySqlASTVisitorAdapter() {
                @Override
                public boolean visit(SQLExprTableSource x) {
                    String tableName = x.getTableName();
                    if (tableName != null) {
                        tableName = SQLUtils.normalize(tableName);
                        MetadataManager.SimpleRoute normalTable = normalTables.get(tableName);
                        if (normalTable != null) {
                            String schema = Optional.ofNullable(x.getSchema()).orElse(dataContext.getDefaultSchema());
                            if (normalTable.getSchemaName().equalsIgnoreCase(schema)) {
                                x.setSimpleName(normalTable.getTableName());
                                x.setSchema(normalTable.getSchemaName());
                                targetName[0] = normalTable.getTargetName();
                            }
                        }
                    }
                    return super.visit(x);
                }
            });
            response.proxySelect(targetName[0], selectStatement.toString());
        } else {
            DrdsRunner drdsRunner = MetaClusterCurrent.wrapper(DrdsRunner.class);
            drdsRunner.runOnDrds(dataContext, selectStatement, response);
        }
    }
}
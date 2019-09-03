package io.mycat.router2;

import cn.lightfish.sqlEngine.ast.SQLParser;
import cn.lightfish.sqlEngine.ast.extractor.Extractors;
import cn.lightfish.sqlEngine.ast.extractor.SchemaTablePair;
import cn.lightfish.sqlEngine.schema.StatementType;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLSelectStatement;
import io.mycat.beans.mycat.MycatSchema;
import io.mycat.beans.mycat.MycatTable;
import io.mycat.router.MycatRouterConfig;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MycatRouter2 {
    final MycatRouterConfig routerConfig;

    public MycatRouter2(MycatRouterConfig routerConfig) {
        this.routerConfig = routerConfig;
    }

    public Object enterRoute(MycatSchema defaultSchema, String sqls) {
        List<SQLStatement> statements = SQLParser.INSTANCE.parse(sqls);

        String dataNode = tryComputeRouteResultOnTableNameOrSchemaName(defaultSchema, statements);

        for (SQLStatement statement : statements) {
            StatementType statementType = Extractors.getStatementType(statement);
            switch (statementType) {
                case SQLSelectStatement: {

                }
                case SQLInsertStatement:
                case MySqlInsertStatement:
                case MySqlUpdateStatement:
                case SQLDeleteStatement:
                case MySqlDeleteStatement:

                default:
            }
            System.out.println(statementType);
        }
        return null;
    }

    private String tryComputeRouteResultOnTableNameOrSchemaName(MycatSchema defaultSchema, List<SQLStatement> statements) {
        Set<SchemaTablePair> tables = new HashSet<>();
        boolean select = true;
        for (SQLStatement statement : statements) {
            if (select) {
                select = (statement instanceof SQLSelectStatement);
            }
            Set<SchemaTablePair> set = Extractors.getTables(defaultSchema.getSchemaName(), statement);
            if (set != null) {
                tables.addAll(set);
            }
        }

        if (tables.isEmpty()) {
            return defaultSchema.getDefaultDataNode();
        }

        Set<String> dataNodes = new HashSet<>();

        for (SchemaTablePair tablePair : tables) {
            MycatSchema schema = routerConfig.getSchemaBySchemaName(tablePair.getSchemaName());
            Map<String, MycatTable> mycatTables = schema.getMycatTables();
            switch (schema.getSchemaType()) {
                case DB_IN_ONE_SERVER: {
                    dataNodes.add(schema.getDefaultDataNode());
                    break;
                }
                case DB_IN_MULTI_SERVER: {
                    MycatTable mycatTable = mycatTables.get(tablePair.getTableName());
                    if (mycatTable != null) {
                        dataNodes.add(mycatTable.getDataNodes().get(0));
                    }
                    break;
                }
                case ANNOTATION_ROUTE: {
                    MycatTable table = schema.getTableByTableName(tablePair.getTableName());
                    switch (table.getType()) {
                        case GLOBAL: {
                            if (select) {
                                boolean found = false;
                                for (String dataNode : table.getDataNodes()) {
                                    if(dataNodes.contains(dataNode)){
                                        found = true;
                                    }
                                }
                                if (!found){

                                }

                            }else {

                            }
                            break;
                        }
                        case SHARING_DATABASE:
                            break;
                        case SHARING_TABLE:
                            break;
                        case SHARING_DATABASE_TABLE:
                            break;
                        case ER:
                            break;
                    }
                    break;
                }
                case SQL_PARSE_ROUTE:
                    break;
            }
        }


        return null;
    }

}
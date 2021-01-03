//package io.mycat.sqlhandler.dql;
//
//import com.alibaba.druid.DbType;
//import com.alibaba.druid.interpreter.TypeCalculation;
//import com.alibaba.druid.sql.SQLUtils;
//import com.alibaba.druid.sql.ast.SQLDataType;
//import com.alibaba.druid.sql.ast.SQLExpr;
//import com.alibaba.druid.sql.ast.expr.*;
//import com.alibaba.druid.sql.ast.statement.*;
//import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
//import com.alibaba.druid.sql.optimizer.rules.TableSourceExtractor;
//import io.mycat.*;
//import io.mycat.beans.mycat.ResultSetBuilder;
//import io.mycat.sqlhandler.ExecuteCode;
//import io.mycat.sqlhandler.SQLRequest;
//import io.mycat.sqlhandler.ShardingSQLHandler;
//import io.mycat.util.NameMap;
//import io.mycat.Response;
//import lombok.Getter;
//
//import java.sql.JDBCType;
//import java.util.*;
//import java.util.function.Predicate;
//
//public class SelectSQLHandler extends ShardingSQLHandler {
//    //    public static String NULL = new String(new char[]{(char)0XFB});
////    public static int NULL = 0XFB;
//    public static String NULL = "NULL";
//
//    public SelectSQLHandler() {
//    }
//
//
//    protected ExecuteCode onSelectNoTable(SQLRequest<SQLSelectStatement> request, Response receiver) {
//        return onSelectDual(request, receiver);
//    }
//
//    /**
//     * impl example
//     * select @@last_insert_id, max(1+1),1+2 as b ,'' as b, '3' as c , null as d from dual;
//     *
//     * @param request
//     * @param receiver
//     * @return
//     */
//    protected ExecuteCode onSelectDual(SQLRequest<SQLSelectStatement> request, Response receiver) {
//        SQLSelectQueryBlock queryBlock = (SQLSelectQueryBlock) (request.getAst().getSelect().getQuery());
//        List<SQLSelectItem> selectItems = queryBlock.getSelectList();
//
//        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
//        List<Object> payloadList = new ArrayList<>();
//        for (SQLSelectItem selectItem : selectItems) {
//            SQLExpr expr = selectItem.getExpr();
//            SQLDataType dataType = expr.computeDataType();
//            if (expr instanceof SQLIdentifierExpr) {
//                receiver.sendError(new MycatException("no support field query. field={} ", expr));
//                return ExecuteCode.PROXY_ERROR;
//            } else if (expr instanceof SQLVariantRefExpr) {
//                receiver.sendError(new MycatException("no support variable. field={} ", expr));
//                return ExecuteCode.PROXY_ERROR;
//            }
//
//            boolean isNull = dataType == null;
//            int dataTypeInt;
//            Object payload;
//            String column = normalize(selectItem.getAlias());
//            if (isNull) {
//                dataTypeInt = JDBCType.NULL.getVendorTypeNumber();
//                payload = null;
//            } else if ((dataType.isInt() || dataType.isNumberic()) && !(expr instanceof SQLNumericLiteralExpr)) {//数学计算
//                dataTypeInt = dataType.jdbcType();
//                if (column == null) {
//                    column = expr.toString();
//                }
//                try {
//                    payload = TypeCalculation.calculateLiteralValue(expr.toString(), Collections.emptyMap());
//                } catch (java.lang.UnsupportedOperationException e) {
//                    receiver.sendError(new MycatException("no support variable calculate. field={} ", expr));
//                    return ExecuteCode.PROXY_ERROR;
//                }
//            } else {
//                dataTypeInt = dataType.jdbcType();
//                payload = ((SQLValuableExpr) expr).getValue();
//            }
//
//            if (column == null) {
//                column = payload == null ? NULL : payload.toString();
//            }
//            resultSetBuilder.addColumnInfo(column, dataTypeInt);
//            payloadList.add(payload);
//        }
//        resultSetBuilder.addObjectRowPayload(payloadList);
//        receiver.sendResultSet(() -> resultSetBuilder.build());
//        return ExecuteCode.PERFORMED;
//    }
//
//
//    private TableHandler chooseTableHandler(NameMap<TableHandler> tableMap, Set<String> tables) {
//        for (String table : tables) {
//            TableHandler tableHandler = tableMap.get(table);
//            if (tableHandler != null) {
//                return tableHandler;
//            }
//        }
//        return null;
//    }
//
//    @Override
//    protected void onExecute(SQLRequest<SQLSelectStatement> request, MycatDataContext dataContext, Response response) throws Exception {
//        //直接调用已实现好的
//        SQLSelectStatement ast = request.getAst();
//        super.onExecute(request, dataContext, response);
//    }
//
//    private boolean hanldeInformationSchema(Response response, SQLSelectStatement ast, TableSourceExtractor tableSourceExtractor) {
//
//        ast.accept(tableSourceExtractor);
//        boolean cantainsInformation_schema = tableSourceExtractor.getTableSources().stream().anyMatch(new Predicate<SQLExprTableSource>() {
//            @Override
//            public boolean test(SQLExprTableSource sqlExprTableSource) {
//                SQLExpr expr = sqlExprTableSource.getExpr();
//                if (expr instanceof SQLPropertyExpr) {
//                    SQLExpr owner = ((SQLPropertyExpr) expr).getOwner();
//                    if (owner instanceof SQLIdentifierExpr) {
//                        return "information_schema".equalsIgnoreCase(((SQLIdentifierExpr) owner).normalizedName());
//                    }
//                    return "information_schema".equalsIgnoreCase(((SQLPropertyExpr) expr).getName());
//
//                }
//                return false;
//            }
//        });
////        if (cantainsInformation_schema) {
////
////            try (DefaultConnection connection = JdbcRuntime.INSTANCE.getConnection(ReplicaSelectorRuntime.INSTANCE.getPrototypeOrFirstReplicaDataSource())) {
////                try (RowBaseIterator rowBaseIterator = connection.executeQuery(ast.toString())) {
////                    response.sendResultSet(() -> rowBaseIterator);
////                    return true;
////                }
////            }
////        }
//        return false;
//    }
//
//    @Getter
//    private static class ASTCheckCollector extends MySqlASTVisitorAdapter {
//        /*select * from db1.table1,db2.table2*/
//        private final Set<SQLExprTableSource> tableSources = new LinkedHashSet<>();
//        private final Set<String> tables = new LinkedHashSet<>();
//        private final SQLSelectStatement statement;
//        private String schema;
//        private List<MycatException> errors = new ArrayList<>();
//        private boolean dual = false;
//
//        public ASTCheckCollector(SQLSelectStatement statement) {
//            this.statement = statement;
//        }
//
//        public void endVisit() {
////            if(this.schema == null || this.schema.isEmpty()){
////                this.errors.add(new MycatException("unknown schema. sql={};\n", statement));
////            }
//            if (this.dual && tables.size() > 1) {
//                this.errors.add(new MycatException("only support one simple dual. no support multiple table. sql={};\n", statement));
//            }
//        }
//
//        @Override
//        public boolean visit(SQLExprTableSource tableSource) {
//            String visitSchema = normalizeAndLowerCase(tableSource.getSchema());
//            if (visitSchema != null) {
//                if (this.schema == null) {
//                    this.schema = visitSchema;
//                } else if (!Objects.equals(this.schema, visitSchema)) {
//                }
//            }
//
//            String table = normalizeAndLowerCase(tableSource.getTableName());
//            if (!this.dual && "dual".equals(table)) {
//                this.dual = true;
//            }
//            this.tables.add(table);
//            this.tableSources.add(tableSource);
//            return this.errors.isEmpty();
//        }
//
//        private static String normalizeAndLowerCase(String str) {
//            if (str == null) {
//                return null;
//            }
//            return normalize(str);
//        }
//    }
//    public static String normalize(String sql) {
//        if (sql == null) {
//            return null;
//        }
//        if ("''".equals(sql)) {
//            return "";
//        }
//        return SQLUtils.normalize(sql, DbType.mysql);
//    }
//}

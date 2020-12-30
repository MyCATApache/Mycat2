package io.mycat.sqlhandler.dql;

import com.alibaba.druid.sql.dialect.mysql.ast.statement.MySqlShowVariantsStatement;
import io.mycat.MycatDataContext;
import io.mycat.sqlhandler.AbstractSQLHandler;
import io.mycat.sqlhandler.SQLRequest;
import io.mycat.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * chenjunwen
 * 实现ShowVariants
 */

public class ShowVariantsSQLHandler extends AbstractSQLHandler<MySqlShowVariantsStatement> {
    private static final Logger LOGGER = LoggerFactory.getLogger(ShowVariantsSQLHandler.class);

    /**
     * 查询
     * GLOBAL_VARIABLES
     * SESSION_VARIABLES
     *
     * @param request
     * @param dataContext
     * @param response
     * @return
     */
//    @Override
//    protected ExecuteCode onExecute(SQLRequest<MySqlShowVariantsStatement> request, MycatDataContext dataContext, Response response) throws Exception {
//        try {
//            MySqlShowVariantsStatement ast = request.getAst();
//
//            boolean global = ast.isGlobal();
//            boolean session = ast.isSession();
//
//            if (!global && !session) {
//                session = true;//如果没有设置则为session
//            }
//            String sql = ShowStatementRewriter.rewriteVariables(ast, "SESSION_VARIABLES");
//
//            InformationSchema informationSchema = (InformationSchema) InformationSchemaRuntime.INSTANCE.get().clone();
//            MycatDBClientMediator client = MycatDBs.createClient(dataContext, new MycatDBClientBasedConfig(MetadataManager.INSTANCE.getSchemaMap(),
//                    Collections.singletonMap("information_schema", informationSchema),false));
//
//            try {
//                //session值覆盖全局值
//                Map<String, Object> globalMap = new HashMap<>();
//                for (Map.Entry<String, Object> stringObjectEntry : RootHelper.INSTANCE.getConfigProvider().globalVariables().entrySet()) {
//                    String key = fixKeyName(stringObjectEntry.getKey());
//                    globalMap.put(key, stringObjectEntry.getValue());
//                }
//
//
//                Map<String, Object> sessionMap = new HashMap<>();
//                for (String k : MycatDBs.VARIABLES_COLUMNNAME_SET) {
//                    String keyName = fixKeyName(k);
//                    Object variable = client.getVariable(k);
//                    sessionMap.put(keyName,variable );
//                    sessionMap.put(keyName.toLowerCase(),variable );
//                    sessionMap.put(keyName.toUpperCase(),variable );
//                }
//                globalMap.putAll(sessionMap);
//
//                ArrayList<InformationSchema.SESSION_VARIABLES_TABLE_OBJECT> list = new ArrayList<>();
//                for (Map.Entry<String, Object> entry : globalMap.entrySet()) {
//                    list.add(InformationSchema
//                            .SESSION_VARIABLES_TABLE_OBJECT
//                            .builder()
//                            .VARIABLE_NAME(entry.getKey())
//                            .VARIABLE_VALUE(entry.getValue() == null ? null : Objects.toString(entry.getValue()))
//                            .build());
//                }
//
//                informationSchema.SESSION_VARIABLES = list.toArray(new InformationSchema.SESSION_VARIABLES_TABLE_OBJECT[0]);
//
//                RowBaseIterator query = client.query(sql);
//
//                response.sendResultSet(() -> query, () -> {
//                    throw new UnsupportedOperationException();
//                });
//            } finally {
//                client.close();
//            }
//        } catch (Throwable e) {
//            LOGGER.error("", e);
//            response.sendError(e);
//        }
//        return ExecuteCode.PERFORMED;
//    }
    @Override
    protected void onExecute(SQLRequest<MySqlShowVariantsStatement> request, MycatDataContext dataContext, Response response) throws Exception {
//        ResultSetBuilder builder = ResultSetBuilder.create();
//        builder.addColumnInfo("Variable_name", JDBCType.VARCHAR);
//        builder.addColumnInfo("Value", JDBCType.VARCHAR);
        response.proxySelectToPrototype(request.getAst().toString());
    }
    @NotNull
    private String fixKeyName(String key) {
        while (true) {
            if (key.startsWith("@")) {
                key = key.substring(1);
            } else {
                break;
            }
        }
        return key;
    }


}

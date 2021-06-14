package io.mycat.calcite;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLCommentHint;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLAllColumnExpr;
import com.alibaba.druid.sql.ast.expr.SQLIdentifierExpr;
import com.alibaba.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.sql.ast.statement.SQLSelectQueryBlock;
import com.alibaba.druid.sql.ast.statement.SQLSelectStatement;
import com.google.common.collect.ImmutableList;
import io.mycat.*;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.beans.mysql.MySQLErrorCode;
import io.mycat.beans.mysql.MySQLType;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.calcite.plan.ObservablePlanImplementorImpl;
import io.mycat.calcite.plan.PlanImplementor;
import io.mycat.calcite.spm.ParamHolder;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.spm.PlanImpl;
import io.mycat.calcite.spm.QueryPlanner;
import io.mycat.calcite.table.MycatLogicTable;
import io.mycat.calcite.table.SchemaHandler;
import io.mycat.util.VertxUtil;
import io.vertx.core.Future;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelWriterImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.jetbrains.annotations.NotNull;

import javax.annotation.Nonnull;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.JDBCType;
import java.util.*;

public class DrdsRunnerHelper {


    public static DrdsSqlWithParams preParse(String sqlStatement, String defaultSchemaName) {
        return preParse(SQLUtils.parseSingleMysqlStatement(sqlStatement), defaultSchemaName);
    }

    public static DrdsSqlWithParams preParse(SQLStatement sqlStatement, String defaultSchemaName) {
        List<Object> params = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        List<String> alias;
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelectQueryBlock firstQueryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getFirstQueryBlock();

            List<String> columnNodes = new ArrayList<String>(firstQueryBlock.getSelectList().size());
            for (SQLSelectItem selectItem : firstQueryBlock.getSelectList()) {
                if (selectItem.getAlias() == null) {
                    if (selectItem.getExpr() instanceof SQLAllColumnExpr) {
                        columnNodes.clear();
                        //break
                        break;
                    } else if (selectItem.getExpr() instanceof SQLPropertyExpr) {
                        if (!"*".equals(((SQLPropertyExpr) selectItem.getExpr()).getName())) {
                            columnNodes.add(SQLUtils.normalize(((SQLPropertyExpr) selectItem.getExpr()).getName()));
                        } else {
                            columnNodes.clear();
                            //break
                            break;
                        }
                    } else if (selectItem.getExpr() instanceof SQLIdentifierExpr) {
                        columnNodes.add(SQLUtils.normalize(((SQLIdentifierExpr) selectItem.getExpr()).getName()));
                    } else {
                        StringBuilder sbText = new StringBuilder();
                        selectItem.output(sbText);
                        columnNodes.add(sbText.toString().replaceAll(" ", ""));
                    }
                } else {
                    columnNodes.add(selectItem.getAlias());
                }
            }
            alias = columnNodes;
        } else {
            alias = Collections.emptyList();
        }
        MutableBoolean complex = new MutableBoolean();
        MycatPreparedStatementUtil.outputToParameterized(sqlStatement,
                defaultSchemaName,
                sb,
                Collections.emptyList(),
                params,
                complex);
        String string = sb.toString();
        return new DrdsSqlWithParams(string,
                params,
                complex.getValue(), getTypes(params), alias, getMycatHints( sqlStatement.getHeadHintsDirect()));
    }

    @NotNull
    private static List<MycatHint> getMycatHints(List<SQLCommentHint> headHintsDirect) {
        List<MycatHint> hints = new LinkedList<>();
        if (headHintsDirect != null) {
            if (!headHintsDirect.isEmpty()) {
                for (SQLCommentHint sqlCommentHint : headHintsDirect) {
                    hints.add(new MycatHint(sqlCommentHint.getText()));
                }
            }else {
                return Collections.emptyList();
            }
        }
        return hints;
    }

    public static List<SqlTypeName> getTypes(List<Object> params) {
        if (params == null || params.isEmpty()) return Collections.emptyList();
        if (params.get(0) instanceof List) {
            return getSqlTypeNames((List) params.get(0));
        } else {
            return getSqlTypeNames(params);
        }
    }

    public static List<SqlTypeName> getSqlTypeNames(List<Object> params) {
        ArrayList<SqlTypeName> list = new ArrayList<>();
        for (Object param : params) {
            if (param == null) {
                list.add(SqlTypeName.NULL);
            } else {
                Class<?> aClass = param.getClass();
                SqlTypeName sqlTypeName = null;
                MySQLType[] mySQLTypes = MySQLType.values();
                for (MySQLType value : mySQLTypes) {
                    if (value.getJavaClass() == aClass) {
                        sqlTypeName = (SqlTypeName.getNameForJdbcType(value.getJdbcType()));
                        break;
                    }
                    if (Integer.class == aClass) {
                        sqlTypeName = SqlTypeName.INTEGER;
                        break;
                    }
                    if (byte[].class == aClass) {
                        sqlTypeName = SqlTypeName.BINARY;
                        break;
                    }
                }
                list.add(Objects.requireNonNull(sqlTypeName, () -> "unknown type :" + param.getClass()));
            }
        }
        return list;
    }


    private RowBaseIterator doExplain(MycatRel relNode1) {
        final StringWriter sw = new StringWriter();
        final RelWriter planWriter = new RelWriterImpl(new PrintWriter(sw), SqlExplainLevel.ALL_ATTRIBUTES, false);
        relNode1.explain(planWriter);
        String[] split = sw.getBuffer().toString().split("\n");
        ResultSetBuilder builder = ResultSetBuilder.create();
        builder.addColumnInfo("explain", JDBCType.VARCHAR);
        for (String s : split) {
            builder.addObjectRowPayload(Arrays.asList(s));
        }
        return builder.build();
    }

    @SneakyThrows
    public static SqlValidator getSqlValidator(DrdsSql drdsSql, CalciteCatalogReader catalogReader) {
        boolean usePrototype = false;
        List<SqlTypeName> suggestedParamTypes = drdsSql.getTypeNames();

        return new SqlValidatorImpl(SqlOperatorTables.chain(catalogReader, MycatCalciteSupport.config.getOperatorTable()), catalogReader, MycatCalciteSupport.TypeFactory,
                MycatCalciteSupport.INSTANCE.getValidatorConfig()) {
            @Override
            protected void inferUnknownTypes(@Nonnull RelDataType inferredType, @Nonnull SqlValidatorScope scope, @Nonnull SqlNode node) {
                if (node != null && node instanceof SqlDynamicParam) {
                    RelDataType relDataType = deriveType(scope, node);
                    return;
                }
                super.inferUnknownTypes(inferredType, scope, node);
            }

//            @Override
//            public RelDataType getUnknownType() {
//                return SqlTypeName.VARCHAR;
//            }

            @Override
            public RelDataType deriveType(SqlValidatorScope scope, SqlNode expr) {
                RelDataType res = resolveDynamicParam(expr);
                if (res == null) {
                    return super.deriveType(scope, expr);
                } else {
                    return res;
                }
            }

            @Override
            public void validateLiteral(SqlLiteral literal) {
                if (literal.getTypeName() == SqlTypeName.DECIMAL) {
                    return;
                }
                super.validateLiteral(literal);
            }

            private RelDataType resolveDynamicParam(SqlNode expr) {
                if (expr != null && expr instanceof SqlDynamicParam) {
                    int index = ((SqlDynamicParam) expr).getIndex();
                    return super.typeFactory.createSqlType(suggestedParamTypes.get(index));
                }
                return null;
            }

            @Override
            public RelDataType getValidatedNodeType(SqlNode node) {
                RelDataType relDataType = resolveDynamicParam(node);
                if (relDataType == null) {
                    return super.getValidatedNodeType(node);
                } else {
                    return relDataType;
                }
            }

            @Override
            public CalciteException handleUnresolvedFunction(SqlCall call, SqlFunction unresolvedFunction, List<RelDataType> argTypes, List<String> argNames) {
                return super.handleUnresolvedFunction(call, unresolvedFunction, argTypes, argNames);
            }

            @Override
            protected void addToSelectList(List<SqlNode> list, Set<String> aliases, List<Map.Entry<String, RelDataType>> fieldList, SqlNode exp, SelectScope scope, boolean includeSystemVars) {
                super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
            }

            @Override
            protected void validateWhereOrOn(SqlValidatorScope scope, SqlNode condition, String clause) {
                if (!condition.getKind().belongsTo(SqlKind.COMPARISON)) {
                    condition = SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO,
                            condition, SqlTypeUtil.convertTypeToSpec(typeFactory.createSqlType(SqlTypeName.BOOLEAN)));
                }
                super.validateWhereOrOn(scope, condition, clause);
            }
        };
    }

    @NotNull
    public static CalciteCatalogReader newCalciteCatalogReader(SchemaPlus plus) {
        CalciteCatalogReader catalogReader = new CalciteCatalogReader(CalciteSchema
                .from(plus),
                ImmutableList.of(),
                MycatCalciteSupport.TypeFactory,
                MycatCalciteSupport.INSTANCE.getCalciteConnectionConfig());
        return catalogReader;
    }


    public static SchemaPlus convertRoSchemaPlus(DrdsConst config) {
        SchemaPlus plus = CalciteSchema.createRootSchema(false).plus();
        List<MycatSchema> schemas = new ArrayList<>();
        for (Map.Entry<String, SchemaHandler> entry : config.schemas().entrySet()) {
            String schemaName = entry.getKey();
            SchemaHandler schemaHandler = entry.getValue();
            Map<String, Table> logicTableMap = new HashMap<>();
            for (TableHandler tableHandler : schemaHandler.logicTables().values()) {
                MycatLogicTable logicTable = new MycatLogicTable(tableHandler);
                logicTableMap.put(logicTable.getTable().getTableName(), logicTable);
            }
            MycatSchema schema = MycatSchema.create(config, schemaName, logicTableMap);
            plus.add(schemaName, schema);
            schemas.add(schema);
        }
        return plus;
    }

    public static Future<Void> impl(Plan plan, PlanImplementor planImplementor) {
        switch (plan.getType()) {
            case PHYSICAL:
                return planImplementor.executeQuery(plan);
            case UPDATE:
                return planImplementor.executeUpdate(plan);
            case INSERT:
                return planImplementor.executeInsert(plan);
            default: {
                return VertxUtil.newFailPromise(new MycatException(MySQLErrorCode.ER_NOT_SUPPORTED_YET, "不支持的执行计划"));
            }
        }
    }

    public static Future<Void> runOnDrds(MycatDataContext dataContext,
                                         SQLSelectStatement sqlSelectStatement, Response response) {

        DrdsSqlWithParams drdsSqlWithParams = DrdsRunnerHelper.preParse(sqlSelectStatement, dataContext.getDefaultSchema());
        PlanImpl plan = getPlan(drdsSqlWithParams);
        PlanImplementor planImplementor = getPlanImplementor(dataContext, response, drdsSqlWithParams);
        return impl(plan, planImplementor);
    }

    @NotNull
    public static PlanImpl getPlan(DrdsSqlWithParams drdsSqlWithParams) {
        QueryPlanner planner = MetaClusterCurrent.wrapper(QueryPlanner.class);
        PlanImpl plan;
        try {
            ParamHolder.CURRENT_THREAD_LOCAL.set(drdsSqlWithParams.getParams());
            CodeExecuterContext codeExecuterContext = planner.innerComputeMinCostCodeExecuterContext(drdsSqlWithParams);
            plan = new PlanImpl(codeExecuterContext.getMycatRel(), codeExecuterContext, drdsSqlWithParams.getAliasList());
        } finally {
            ParamHolder.CURRENT_THREAD_LOCAL.set(null);
        }
        return plan;
    }

    @NotNull
    public static PlanImplementor getPlanImplementor(MycatDataContext dataContext, Response response, DrdsSqlWithParams drdsSqlWithParams) {
        XaSqlConnection transactionSession = (XaSqlConnection) dataContext.getTransactionSession();
        List<Object> params = drdsSqlWithParams.getParams();
        return new ObservablePlanImplementorImpl(
                transactionSession,
                dataContext, drdsSqlWithParams, response);
    }

}
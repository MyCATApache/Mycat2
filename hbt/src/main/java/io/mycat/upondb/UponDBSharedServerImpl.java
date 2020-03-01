package io.mycat.upondb;

import com.alibaba.fastsql.DbType;
import com.alibaba.fastsql.sql.SQLUtils;
import com.alibaba.fastsql.sql.ast.SQLExpr;
import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.fastsql.sql.ast.statement.*;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlDeleteStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlExplainStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlInsertStatement;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlUpdateStatement;
import com.alibaba.fastsql.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.alibaba.fastsql.sql.visitor.SQLASTOutputVisitor;
import com.alibaba.fastsql.support.calcite.CalciteMySqlNodeVisitor;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilderImpl;
import io.mycat.beans.mycat.UpdateRowMetaData;
import io.mycat.calcite.CalciteRowMetaData;
import io.mycat.calcite.MycatCalciteContext;
import io.mycat.calcite.MycatCalcitePlanner;
import io.mycat.calcite.metadata.MetadataManager;
import io.mycat.calcite.prepare.*;
import io.mycat.hbt.SchemaConvertor;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.hbt.ast.modify.MergeModify;
import io.mycat.hbt.ast.modify.ModifyFromSql;
import io.mycat.hbt.parser.HBTParser;
import io.mycat.hbt.parser.ParseNode;
import lombok.Getter;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.jetbrains.annotations.NotNull;

import java.sql.Types;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

public class UponDBSharedServerImpl implements UponDBSharedServer {
    private static final AtomicLong PREPARE_ID_GENERATOR = new AtomicLong(0);
    private final Map<Long, PrepareObject> PREPARE_MAP = new HashMap<>();
    private static final ConcurrentHashMap<Byte, Object> singletons = new ConcurrentHashMap<>();

    public UponDBSharedServerImpl() {
    }

    public <T> T getComponent(Byte key, Function<Byte, T> factory) {
        return (T) singletons.computeIfAbsent(key, factory);
    }

    public <T> T replaceComponent(Byte key, Function<Byte, T> factory) {
        return (T) singletons.replace(key, factory.apply(key));
    }

    @Override
    public PrepareObject prepare(String sql, UponDBContext dbContext) {
        return PREPARE_MAP.computeIfAbsent(PREPARE_ID_GENERATOR.incrementAndGet(),
                id -> prepare(sql, id, dbContext));
    }

    private PrepareObject prepare(String sql, Long id, UponDBContext dbContext) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);//不支持多语句的预处理
        return prepare(sql, id, sqlStatement, dbContext);
    }

    @Override
    public RowBaseIterator execute(Long id, List<Object> params, UponDBContext dbContext) {
        return PREPARE_MAP.get(id).plan(params).run();
    }

    @Override
    public Iterator<RowBaseIterator> executeSqls(String sql, UponDBContext dbContext) {
        List<SQLStatement> statements = SQLUtils.parseStatements(sql, DbType.mysql);
        List<MycatSQLPrepareObject> collect = new ArrayList<>();
        for (SQLStatement statement : statements) {
            MycatSQLPrepareObject query = query(sql, statement, dbContext);
            if (query != null) {
                collect.add(query);
            } else {
                throw new UnsupportedOperationException();
            }
        }
        Iterator<MycatSQLPrepareObject> iterator = collect.iterator();
        return new Iterator<RowBaseIterator>() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public RowBaseIterator next() {
                return iterator.next().plan(Collections.emptyList()).run();
            }
        };
    }


    @NotNull
    private MycatSQLPrepareObject prepare(String templateSql, Long id, SQLStatement sqlStatement, UponDBContext dbContext) {
        String schema = dbContext.getSchema();
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock();
            SQLTableSource from = queryBlock.getFrom();
            if (from != null) {
                return complieQuery(templateSql, id, sqlStatement, dbContext);
            }
        }
        MetadataManager.INSTANCE.resolveMetadata(sqlStatement);
        int variantRefCount = getVariantRefCount(sqlStatement);
        BiFunction<String, String, Iterator> function = null;
        if (sqlStatement instanceof MySqlInsertStatement) {
            function = insertHandler(dbContext.getSchema());
        }
        if (sqlStatement instanceof MySqlUpdateStatement || sqlStatement instanceof MySqlDeleteStatement) {
            function = updateHandler(schema);
        }
        if (function != null) {
            function = updateHandler(schema);
        }
        return getMycatPrepareObject(dbContext, templateSql, id, sqlStatement, variantRefCount, function);
    }

    @NotNull
    private BiFunction<String, String, Iterator> insertHandler(String defaultSchemaName) {
        return (s, s2) -> MetadataManager.INSTANCE.getInsertInfoMap(defaultSchemaName, s2).entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
    }

    @NotNull
    private BiFunction<String, String, Iterator> updateHandler(String defaultSchemaName) {
        return (s, s2) -> MetadataManager.INSTANCE.rewriteSQL(defaultSchemaName, s2).entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
    }


    @Override
    public RowBaseIterator query(String sql, UponDBContext dbContext) {
        return Objects.requireNonNull(query(sql,
                SQLUtils.parseSingleMysqlStatement(sql), dbContext)).plan(Collections.emptyList()).run();
    }

    @Override
    public UpdateRowIterator update(String sql, UponDBContext dbContext) {
        return null;
    }

    @Override
    public UpdateRowIterator loadData(String sql, UponDBContext dbContext) {
        return null;
    }

    @Override
    public RowBaseIterator executeRel(String hbt, UponDBContext dbContext) {
        return prepareHbt(hbt, null, dbContext).plan(Collections.emptyList()).run();
    }

    @NotNull
    private MycatSQLPrepareObject explain(String sql, MySqlExplainStatement sqlStatement, UponDBContext dbContext) {
        return new QueryPlanRunnerImpl(dbContext, sql) {
            @Override
            public PlanRunner plan(List<Object> params) {
                return null;
            }

            @Override
            public RowBaseIterator run() {
                String sql = sqlStatement.getStatement().toString();
                return explainSql(sql, dbContext);
            }

            @Override
            public void innerEun() {

            }

            @Override
            public String innerExplain() {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<String> explain() {
                throw new UnsupportedOperationException();
            }
        };
    }

    private MycatSQLPrepareObject query(String sql, SQLStatement sqlStatement, UponDBContext dbContext) {
        boolean ddl = sqlStatement instanceof SQLSelectStatement || sqlStatement instanceof MySqlInsertStatement
                || sqlStatement instanceof MySqlUpdateStatement || sqlStatement instanceof MySqlDeleteStatement;
        if (ddl) {
            return complieQuery(sql, null, sqlStatement, dbContext);
        }
        if (sqlStatement instanceof SQLCommitStatement) return commit(sql, dbContext);
        if (sqlStatement instanceof SQLRollbackStatement) return (rollback(sql, dbContext));
        if (sqlStatement instanceof SQLSetStatement) {
            return setStatement(sql, (SQLSetStatement) sqlStatement, dbContext);
        }
        if (sqlStatement instanceof SQLUseStatement) {
            String normalize = SQLUtils.normalize(((SQLUseStatement) sqlStatement).getDatabase().getSimpleName());
            return use(sql, normalize, dbContext);
        }
        if (sqlStatement instanceof MySqlExplainStatement) {
            return explain(sql, (MySqlExplainStatement) sqlStatement, dbContext);
        }
        return null;
    }


    @NotNull
    private MycatSQLPrepareObject setStatement(String sql, SQLSetStatement sqlStatement, UponDBContext uponDBContext) {
        return new QueryPlanRunnerImpl(uponDBContext, sql) {

            @Override
            public void innerEun() {
                for (SQLAssignItem item : sqlStatement.getItems()) {
                    String target = Objects.toString(item.getTarget());
                    SQLExpr value = item.getValue();
                    uponDBContext.set(target, value);
                }
            }

            @Override
            public String innerExplain() {
                return sql;
            }
        };
    }

    @NotNull
    private MycatSQLPrepareObject use(String sql, String normalize, UponDBContext uponDBContext) {
        return new QueryPlanRunnerImpl(uponDBContext, sql) {

            @Override
            public void innerEun() {
                uponDBContext.useSchema(normalize);
            }

            @Override
            public String innerExplain() {
                return "use " + normalize;
            }
        };
    }

    @NotNull
    private MycatSQLPrepareObject rollback(String sql, UponDBContext uponDBContext) {
        return new QueryPlanRunnerImpl(uponDBContext, sql) {
            @Override
            public void innerEun() {
                uponDBContext.rollback();
            }

            @Override
            public String innerExplain() {
                return "rollback";
            }
        };
    }

    @NotNull
    private MycatSQLPrepareObject commit(String sql, UponDBContext uponDBContext) {
        return new QueryPlanRunnerImpl(uponDBContext, sql) {
            @Override
            public void innerEun() {
                uponDBContext.commit();
            }

            @Override
            public String innerExplain() {
                return "commit";
            }
        };
    }


    @Override
    public RowBaseIterator explainSql(String sql, UponDBContext uponDBContext) {
        PrepareObject prepare = prepare(sql, uponDBContext);
        ResultSetBuilderImpl resultSetBuilder = ResultSetBuilderImpl.create();
        List<String> explain = prepare.plan(Collections.emptyList()).explain();
        resultSetBuilder.addColumnInfo("plan", Types.VARCHAR);
        for (String s : explain) {
            resultSetBuilder.addObjectRowPayload(new Object[]{s});
        }
        return resultSetBuilder.build();
    }

    @Override
    public void closePrepare(Long id) {
        PREPARE_MAP.remove(id);
    }

    @Override
    public Object get(String target) {
        return null;
    }

    public PrepareObject prepareHbt(String hbt, Long id, UponDBContext dbContext) {
        HBTParser hbtParser = new HBTParser(hbt);
        List<ParseNode> parseNodes = hbtParser.statementList();
        if (parseNodes.size() != 1) {
            throw new UnsupportedOperationException();
        }
        return complieHBT(parseNodes.get(0), id, hbtParser.getParamCount(), dbContext);
    }

    @NotNull
    private MycatSQLPrepareObject complieQuery(String sql, Long id, SQLStatement
            sqlStatement, UponDBContext dataContext) {
        SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock();
        boolean forUpdate = queryBlock.isForUpdate();
        CalciteMySqlNodeVisitor calciteMySqlNodeVisitor = new CalciteMySqlNodeVisitor();
        sqlStatement.accept(calciteMySqlNodeVisitor);
        SqlNode sqlNode = calciteMySqlNodeVisitor.getSqlNode();
        MycatCalcitePlanner planner = MycatCalciteContext.INSTANCE.createPlanner(dataContext);
        SqlValidatorImpl sqlValidator = planner.getSqlValidator();
        sqlNode = sqlValidator.validate(sqlNode);
        MycatRowMetaData parameterRowType = null;
        if (id != null) {
            parameterRowType = new CalciteRowMetaData(sqlValidator.getParameterRowType(sqlNode).getFieldList());
        }
        MycatRowMetaData resultRowType = new CalciteRowMetaData(sqlValidator.getValidatedNodeType(sqlNode).getFieldList());
        return new MycatCalcitePrepare(id, sql, sqlNode, parameterRowType, resultRowType, forUpdate, dataContext);
    }


    private MycatHbtPrepareObject complieMergeModify(Long id, int paramCount, MergeModify mergeModify, UponDBContext dbContext) {
        return new MycatHbtPrepareObject(id, paramCount) {
            @Override
            public MycatRowMetaData resultSetRowType() {
                return UpdateRowMetaData.INSTANCE;
            }

            @Override
            public PlanRunner plan(List<Object> params) {
                return new MycatTextUpdatePrepareObject(id, paramCount, (mycatTextUpdatePrepareObject, list) -> {
                    Iterator<ModifyFromSql> iterator = mergeModify.getList().iterator();
                    return new Iterator<TextUpdateInfo>() {
                        @Override
                        public boolean hasNext() {
                            return iterator.hasNext();
                        }

                        @Override
                        public TextUpdateInfo next() {
                            ModifyFromSql next = iterator.next();
                            return TextUpdateInfo.create(next.getTargetName(), Collections.singletonList(next.getSql()));
                        }
                    };
                }, dbContext).plan(params);
            }
        };
    }

    @NotNull
    private MycatSQLPrepareObject getMycatPrepareObject(
            UponDBContext uponDBContext,
            String templateSql,
            Long id,
            SQLStatement sqlStatement,
            int variantRefCount,
            BiFunction<String, String, Iterator> accept) {
        String schema = uponDBContext.getSchema();//因为客户端的schema会变化
        return new MycatDelegateSQLPrepareObject(id, uponDBContext, templateSql, new MycatTextUpdatePrepareObject(id, variantRefCount, (prepareObject, params) -> {
            StringBuilder out = new StringBuilder();
            SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, DbType.mysql);
            visitor.setInputParameters(params);
            sqlStatement.accept(visitor);
            String sql = out.toString();
            return accept.apply(schema, sql);
        }, uponDBContext));

    }

    private MycatHbtPrepareObject complieHBT(ParseNode parseNode, Long id, int paramCount, UponDBContext dbContext) {
        SchemaConvertor schemaConvertor = new SchemaConvertor();
        Schema originSchema = schemaConvertor.transforSchema(parseNode);
        MycatHbtPrepareObject prepareObject = null;
        switch (originSchema.getOp()) {
            case MODIFY_FROM_SQL: {
                ModifyFromSql originSchema1 = (ModifyFromSql) originSchema;
                MergeModify mergeModify = new MergeModify(Collections.singleton(originSchema1));
                prepareObject = complieMergeModify(id, paramCount, mergeModify, dbContext);
                break;
            }
            case MERGE_MODIFY: {
                MergeModify originSchema1 = (MergeModify) originSchema;
                prepareObject = complieMergeModify(id, paramCount, originSchema1, dbContext);
                break;
            }
            default:
                prepareObject = new MycatHbtCalcitePrepareObject(id, paramCount, originSchema, dbContext);
        }
        return prepareObject;
    }

    @NotNull
    private int getVariantRefCount(SQLStatement sqlStatement) {
        SQLVariantRefExprCounter sqlVariantRefExprCounter = new SQLVariantRefExprCounter();
        sqlStatement.accept(sqlVariantRefExprCounter);
        return sqlVariantRefExprCounter.getCount();
    }

    @Getter
    static class SQLVariantRefExprCounter extends MySqlASTVisitorAdapter {
        final List<SQLVariantRefExpr> sqlVariantRefExprs = new ArrayList<>();

        @Override
        public boolean visit(SQLVariantRefExpr x) {
            if (!x.isSession() && !x.isGlobal() && "?".equals(x.getName())) {
                sqlVariantRefExprs.add(x);
            }
            return super.visit(x);
        }

        public int getCount() {
            return sqlVariantRefExprs.size();
        }
    }

}
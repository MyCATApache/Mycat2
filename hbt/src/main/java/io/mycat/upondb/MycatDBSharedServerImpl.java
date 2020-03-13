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
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.prepare.*;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import io.mycat.hbt.HBTRunners;
import io.mycat.hbt.TextUpdateInfo;
import io.mycat.metadata.MetadataManager;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.jetbrains.annotations.NotNull;

import java.sql.Types;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;

@NoArgsConstructor
public class MycatDBSharedServerImpl implements MycatDBSharedServer {
    private static final AtomicLong PREPARE_ID_GENERATOR = new AtomicLong(0);
    private final Map<Long, PrepareObject> PREPARE_MAP = new HashMap<>();
    private static final ConcurrentHashMap<Byte, Object> singletons = new ConcurrentHashMap<>();

    public <T> T getComponent(Byte key, Function<Byte, T> factory) {
        return (T) singletons.computeIfAbsent(key, factory);
    }

    public <T> T replaceComponent(Byte key, Function<Byte, T> factory) {
        return (T) singletons.replace(key, factory.apply(key));
    }

    @Override
    public PrepareObject prepare(String sql, MycatDBContext dbContext) {
        return PREPARE_MAP.computeIfAbsent(PREPARE_ID_GENERATOR.incrementAndGet(),
                id -> prepare(sql, id, dbContext));
    }

    private PrepareObject prepare(String sql, Long id, MycatDBContext dbContext) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(sql);//不支持多语句的预处理
        return prepare(sql, id, sqlStatement, dbContext);
    }

    @Override
    public RowBaseIterator execute(Long id, List<Object> params, MycatDBContext dbContext) {
        return PREPARE_MAP.get(id).plan(params).run();
    }

    @Override
    public Iterator<RowBaseIterator> executeSqls(String sql, MycatDBContext dbContext) {
        List<SQLStatement> statements = SQLUtils.parseStatements(sql, DbType.mysql);
        List<MycatSQLPrepareObject> collect = new ArrayList<>();
        for (SQLStatement statement : statements) {
            MycatSQLPrepareObject query = prepare(sql, null, statement, dbContext);
            collect.add(query);
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
    private MycatSQLPrepareObject prepare(String templateSql, Long id, SQLStatement sqlStatement, MycatDBContext dbContext) {
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
        } else if (sqlStatement instanceof MySqlUpdateStatement || sqlStatement instanceof MySqlDeleteStatement) {
            function = updateHandler(schema);
        }
        if (function == null) {
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
    public RowBaseIterator query(String sql, MycatDBContext dbContext) {
        return Objects.requireNonNull(query(sql,
                SQLUtils.parseSingleMysqlStatement(sql), dbContext)).plan(Collections.emptyList()).run();
    }

    @Override
    public UpdateRowIteratorResponse update(String sql, MycatDBContext dbContext) {
        return (UpdateRowIteratorResponse) prepare(sql, null, dbContext).plan(Collections.emptyList()).run();
    }

    @Override
    public UpdateRowIteratorResponse loadData(String sql, MycatDBContext dbContext) {
        Iterator<RowBaseIterator> rowBaseIteratorIterator = executeSqls(sql, dbContext);
        long lastInsertId = 0;
        long updateCount = 0;
        while (rowBaseIteratorIterator.hasNext()) {
            RowBaseIterator next = rowBaseIteratorIterator.next();
            if (next instanceof UpdateRowIteratorResponse) {
                UpdateRowIteratorResponse next1 = (UpdateRowIteratorResponse) next;
                lastInsertId += next1.getLastInsertId();
                updateCount += next1.getUpdateCount();
            }
        }
        return new UpdateRowIteratorResponse(updateCount, lastInsertId, dbContext.getServerStatus());
    }

    @Override
    public RowBaseIterator executeRel(String hbt, MycatDBContext dbContext) {
        HBTRunners hbtRunners = new HBTRunners(dbContext);
        return hbtRunners.run(hbt);
    }

    @Override
    public List<String> explain(String sql, MycatDBContext dbContext) {
        return explainSqlAsListString(sql, dbContext);
    }

    @Override
    public List<String> explainRel(String sql, MycatDBContext dbContext) {
        HBTRunners hbtRunners = new HBTRunners(dbContext);
        return hbtRunners.prepareHBT(null, sql).plan(Collections.emptyList()).explain();
    }

    @NotNull
    private MycatSQLPrepareObject explain(String sql, MySqlExplainStatement sqlStatement, MycatDBContext dbContext) {
        return new SimpleSQLPrepareObjectPlanner(dbContext, sql) {
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

    private MycatSQLPrepareObject query(String sql, SQLStatement sqlStatement, MycatDBContext dbContext) {
        boolean ddl = sqlStatement instanceof SQLSelectStatement || sqlStatement instanceof MySqlInsertStatement
                || sqlStatement instanceof MySqlUpdateStatement || sqlStatement instanceof MySqlDeleteStatement;
        if (ddl) {
            return prepare(sql, null, null, dbContext);
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
    private MycatSQLPrepareObject setStatement(String sql, SQLSetStatement sqlStatement, MycatDBContext uponDBContext) {
        return new SimpleSQLPrepareObjectPlanner(uponDBContext, sql) {

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
    private MycatSQLPrepareObject use(String sql, String normalize, MycatDBContext uponDBContext) {
        return new SimpleSQLPrepareObjectPlanner(uponDBContext, sql) {

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
    private MycatSQLPrepareObject rollback(String sql, MycatDBContext uponDBContext) {
        return new SimpleSQLPrepareObjectPlanner(uponDBContext, sql) {
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
    private MycatSQLPrepareObject commit(String sql, MycatDBContext uponDBContext) {
        return new SimpleSQLPrepareObjectPlanner(uponDBContext, sql) {
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


    public List<String> explainSqlAsListString(String sql, MycatDBContext uponDBContext) {
        PrepareObject prepare = prepare(sql, uponDBContext);
        return prepare.plan(Collections.emptyList()).explain();
    }

    public RowBaseIterator explainSql(String sql, MycatDBContext uponDBContext) {
        List<String> explain = explainSqlAsListString(sql, uponDBContext);
        ResultSetBuilder resultSetBuilder = ResultSetBuilder.create();
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


    @NotNull
    private MycatSQLPrepareObject complieQuery(String sql, Long id, SQLStatement
            sqlStatement, MycatDBContext dataContext) {
        SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock();
        boolean forUpdate = queryBlock.isForUpdate();
        CalciteMySqlNodeVisitor calciteMySqlNodeVisitor = new CalciteMySqlNodeVisitor();
        sqlStatement.accept(calciteMySqlNodeVisitor);
        SqlNode sqlNode = calciteMySqlNodeVisitor.getSqlNode();
        MycatCalcitePlanner planner = MycatCalciteSupport.INSTANCE.createPlanner(dataContext);
        SqlValidatorImpl sqlValidator = planner.getSqlValidator();
        sqlNode = sqlValidator.validate(sqlNode);
        MycatRowMetaData parameterRowType = null;
        if (id != null) {
            parameterRowType = new CalciteRowMetaData(sqlValidator.getParameterRowType(sqlNode).getFieldList());
        }
        MycatRowMetaData resultRowType = new CalciteRowMetaData(sqlValidator.getValidatedNodeType(sqlNode).getFieldList());
        return new MycatCalciteSQLPrepareObject(id, sql, sqlNode, parameterRowType, resultRowType, forUpdate, dataContext);
    }


    @NotNull
    private MycatSQLPrepareObject getMycatPrepareObject(
            MycatDBContext uponDBContext,
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
            Iterator apply = accept.apply(schema, sql);
            return apply;
        }, uponDBContext));

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
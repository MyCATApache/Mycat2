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
import com.alibaba.fastsql.sql.repository.SchemaObject;
import com.alibaba.fastsql.sql.visitor.SQLASTOutputVisitor;
import io.mycat.MycatConnection;
import io.mycat.PlanRunner;
import io.mycat.TextUpdateInfo;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.api.collector.UpdateRowIteratorResponse;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilder;
import io.mycat.calcite.MycatCalciteMySqlNodeVisitor;
import io.mycat.calcite.prepare.*;
import io.mycat.hbt.HBTRunners;
import io.mycat.metadata.MetadataManager;
import io.mycat.ParseContext;
import io.mycat.metadata.SchemaHandler;
import io.mycat.TableHandler;
import io.mycat.replica.ReplicaSelectorRuntime;
import lombok.Getter;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlNode;
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
    public MycatSQLPrepareObject prepare(String templateSql, Long id, SQLStatement sqlStatement, MycatDBContext dbContext) {
        String defaultSchema = dbContext.getSchema();
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock();
            SQLTableSource from = queryBlock.getFrom();
            if (from != null) {
                return complieQuery(templateSql, id, sqlStatement, dbContext);
            } else {
                return getPrepareObject(templateSql, dbContext);
            }
        }
        MetadataManager.INSTANCE.resolveMetadata(sqlStatement);
        int variantRefCount = getVariantRefCount(sqlStatement);
        Function<ParseContext, Iterator<TextUpdateInfo>> handler = null;
        if (sqlStatement instanceof MySqlInsertStatement) {
            MySqlInsertStatement insertStatement = (MySqlInsertStatement) sqlStatement;
            SchemaObject schemaObject = (insertStatement).getTableSource().getSchemaObject();
            String schema = SQLUtils.normalize(Optional.ofNullable(schemaObject).map(i -> i.getSchema()).map(i -> i.getName()).orElse(defaultSchema));
            String tableName = SQLUtils.normalize(insertStatement.getTableName().getSimpleName());
            TableHandler logicTable = getLogicTable(schema, tableName);
            handler = logicTable.insertHandler();
        } else if (sqlStatement instanceof MySqlUpdateStatement) {
            SQLExprTableSource tableSource = (SQLExprTableSource) ((MySqlUpdateStatement) sqlStatement).getTableSource();
            SchemaObject schemaObject = tableSource.getSchemaObject();
            String schema = SQLUtils.normalize(Optional.ofNullable(schemaObject).map(i -> i.getSchema()).map(i -> i.getName()).orElse(defaultSchema));
            String tableName = SQLUtils.normalize(((MySqlUpdateStatement) sqlStatement).getTableName().getSimpleName());
            TableHandler logicTable = getLogicTable(schema, tableName);
            handler = logicTable.updateHandler();
        } else if (sqlStatement instanceof MySqlDeleteStatement) {
            SQLExprTableSource tableSource = (SQLExprTableSource) ((MySqlDeleteStatement) sqlStatement).getTableSource();
            SchemaObject schemaObject = tableSource.getSchemaObject();
            String schema = SQLUtils.normalize(Optional.ofNullable(schemaObject).map(i -> i.getSchema()).map(i -> i.getName()).orElse(defaultSchema));
            String tableName = SQLUtils.normalize(((MySqlDeleteStatement) sqlStatement).getTableName().getSimpleName());
            TableHandler logicTable = getLogicTable(schema, tableName);
            handler = logicTable.deleteHandler();
        }
        if (handler == null) {
            throw new UnsupportedOperationException();
        }
        return getMycatPrepareObject(dbContext, templateSql, id, sqlStatement, variantRefCount, handler);
    }

    @NotNull
    private TableHandler getLogicTable(String schema, String tableName) {
        SchemaHandler schemaHandler = MetadataManager.INSTANCE.getSchemaMap().get(schema);
        return Objects.requireNonNull(Objects.requireNonNull(schemaHandler, "schema is not existed").logicTables().get(tableName), "table is not existed");
    }

//    @AllArgsConstructor
//    static class P implements PlanRunner {
//        final MycatDBContext dbContext;
//        final String sql;
//
//        @Override
//        public List<String> explain() {
//            return Arrays.asList("direct query sql:", sql);
//        }
//
//        @Override
//        public RowBaseIterator run() {
//            return dbContext.queryDefaultTarget(sql);
//        }
//    }

    @NotNull
    private MycatSQLPrepareObject getPrepareObject(String templateSql, MycatDBContext dbContext) {
        return new MycatSQLPrepareObject(null, dbContext, templateSql, false) {

            @Override
            public MycatRowMetaData prepareParams() {
                return null;
            }

            @Override
            public MycatRowMetaData resultSetRowType() {
                return null;
            }


            @Override
            public PlanRunner plan(List<Object> params) {
                return new PlanRunner (){
                    @Override
                    public List<String> explain() {
                        return Arrays.asList("direct query sql:", templateSql);
                    }

                    @Override
                    public RowBaseIterator run() {
                        String firstReplicaDataSource = ReplicaSelectorRuntime.INSTANCE.getFirstReplicaDataSource();
                        MycatConnection connection = dbContext.getConnection(firstReplicaDataSource);
                        return connection.executeQuery(null,templateSql);
                    }
                };
            }
        };
    }

    @NotNull
    private BiFunction<String, String, Iterator> insertHandler(String defaultSchemaName) {
        return (s, s2) -> {
            return MetadataManager.INSTANCE.getInsertInfoMap(defaultSchemaName, s2).entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
        };
    }

    @NotNull
    private BiFunction<String, String, Iterator> updateHandler(String defaultSchemaName) {
        return (s, s2) -> MetadataManager.INSTANCE.rewriteSQL(defaultSchemaName, s2).entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
    }


    @Override
    public RowBaseIterator query(String sql, MycatDBContext dbContext) {
        return Objects.requireNonNull(innerQueryPrepareObject(sql, dbContext)).plan(Collections.emptyList()).run();
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

    public MycatSQLPrepareObject innerQueryPrepareObject(String sql, MycatDBContext dbContext) {
        return query(sql, SQLUtils.parseSingleMysqlStatement(sql), dbContext);
    }

    @Override
    public MycatTextUpdatePrepareObject innerUpdatePrepareObject(String sql, MycatDBContext dbContext) {
        MycatDelegateSQLPrepareObject prepare = (MycatDelegateSQLPrepareObject) prepare(sql, null, dbContext);
        return (MycatTextUpdatePrepareObject) prepare.getPrepareObject();
    }

    public MycatSQLPrepareObject query(String sql, SQLStatement sqlStatement, MycatDBContext dbContext) {
        boolean ddl = sqlStatement instanceof SQLSelectStatement || sqlStatement instanceof MySqlInsertStatement
                || sqlStatement instanceof MySqlUpdateStatement || sqlStatement instanceof MySqlDeleteStatement;
        if (ddl) {
            return prepare(sql, null, sqlStatement, dbContext);
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
                    uponDBContext.setVariable(target, value);
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
            resultSetBuilder.addObjectRowPayload(s);
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
        MycatCalciteMySqlNodeVisitor calciteMySqlNodeVisitor = new MycatCalciteMySqlNodeVisitor();
        sqlStatement.accept(calciteMySqlNodeVisitor);
        SqlNode sqlNode = calciteMySqlNodeVisitor.getSqlNode();
        return new FastMycatCalciteSQLPrepareObject(id, sql, sqlNode, null, null, forUpdate, dataContext);
    }


    @NotNull
    private MycatSQLPrepareObject getMycatPrepareObject(
            MycatDBContext uponDBContext,
            String templateSql,
            Long id,
            SQLStatement sqlStatement,
            int variantRefCount,
            Function<ParseContext, Iterator<TextUpdateInfo>> accept) {
        return new MycatDelegateSQLPrepareObject(id, uponDBContext, templateSql, new MycatTextUpdatePrepareObject(id, variantRefCount, (prepareObject, params) -> {
            StringBuilder out = new StringBuilder();
            SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, DbType.mysql);
            visitor.setInputParameters(params);
            sqlStatement.accept(visitor);
            String sql = out.toString();
            ParseContext parseContext = new ParseContext();
            parseContext.setSql(sql);
            return accept.apply(parseContext);
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
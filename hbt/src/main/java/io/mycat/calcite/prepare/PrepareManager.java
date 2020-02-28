/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.prepare;


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
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mycat.ResultSetBuilderImpl;
import io.mycat.beans.mycat.UpdateRowMetaData;
import io.mycat.calcite.CalciteRowMetaData;
import io.mycat.calcite.MycatCalciteContext;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.calcite.MycatCalcitePlanner;
import io.mycat.calcite.metadata.MetadataManager;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author Junwen Chen
 **/
public class PrepareManager {
    private static final AtomicLong PREPARE_ID_GENERATOR = new AtomicLong(0);
    private ConcurrentHashMap<Long, MycatPrepareObject> PREPARE_MAP = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, MycatSQLPrepareObject> SQL_PREPASRE = new ConcurrentHashMap<>();


    public MycatPrepareObject prepare(String defaultSchemaName, String sql) {
        return SQL_PREPASRE
                .computeIfAbsent(sql, s ->
                        (MycatSQLPrepareObject) PREPARE_MAP.computeIfAbsent(PREPARE_ID_GENERATOR.incrementAndGet(),
                                id -> prepare(defaultSchemaName, sql, id)));
    }

    public MycatSQLPrepareObject prepare(String defaultSchemaName, String templateSql, Long id) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(templateSql);//不支持多语句的预处理
        return prepare(defaultSchemaName, templateSql, id, sqlStatement);
    }

    public MycatPrepareObject prepare(String hbt) {
        return PREPARE_MAP.computeIfAbsent(PREPARE_ID_GENERATOR.incrementAndGet(), new Function<Long, MycatPrepareObject>() {
            @Override
            public MycatPrepareObject apply(Long aLong) {
                return prepare(hbt, aLong);
            }
        });
    }

    public List<MycatPrepareObject> query(String hbt) {
        HBTParser hbtParser = new HBTParser(hbt);
        List<ParseNode> parseNodes = hbtParser.statementList();
        List<MycatPrepareObject> prepareObjects = new ArrayList<>();
        for (ParseNode parseNode : parseNodes) {
            prepareObjects.add(complieHBT(parseNode, null, hbtParser.getParamCount()));
        }
        return prepareObjects;
    }

    public MycatPrepareObject prepare(String hbt, Long id) {
        HBTParser hbtParser = new HBTParser(hbt);
        List<ParseNode> parseNodes = hbtParser.statementList();
        if (parseNodes.size() != 1) {
            throw new UnsupportedOperationException();
        }
        return complieHBT(parseNodes.get(0), id, hbtParser.getParamCount());
    }

    @NotNull
    private MycatHbtPrepareObject complieHBT(ParseNode parseNode, Long id, int paramCount) {
        Schema originSchema = SchemaConvertor.transforSchema(parseNode);
        MycatHbtPrepareObject prepareObject = null;
        switch (originSchema.getOp()) {
            case MODIFY_FROM_SQL: {
                ModifyFromSql originSchema1 = (ModifyFromSql) originSchema;
                MergeModify mergeModify = new MergeModify(Collections.singleton(originSchema1));
                prepareObject = complieMergeModify(id, paramCount, mergeModify);
                break;
            }
            case MERGE_MODIFY: {
                MergeModify originSchema1 = (MergeModify) originSchema;
                prepareObject = complieMergeModify(id, paramCount, originSchema1);
                break;
            }
            default:
                prepareObject = new MycatHbtCalcitePrepareObjec(id, paramCount, originSchema);
        }
        return prepareObject;
    }

    @NotNull
    private MycatHbtPrepareObject complieMergeModify(Long id, int paramCount, MergeModify mergeModify) {
        MycatHbtPrepareObject prepareObject;
        MycatTextUpdatePrepareObject mycatTextUpdatePrepareObject1 = new MycatTextUpdatePrepareObject(id, paramCount, (mycatTextUpdatePrepareObject, list) -> {
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
        });
        prepareObject = new MycatHbtPrepareObject(id, paramCount) {
            @Override
            public MycatRowMetaData resultSetRowType() {
                return UpdateRowMetaData.INSTANCE;
            }

            @Override
            public PlanRunner plan(List<Object> params) {
                return mycatTextUpdatePrepareObject1.plan(params);
            }
        };
        return prepareObject;
    }

    @NotNull
    private MycatSQLPrepareObject prepare(String defaultSchemaName, String templateSql, Long id, SQLStatement
            sqlStatement) {
        if (sqlStatement instanceof SQLSelectStatement) {
            SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock();
            SQLTableSource from = queryBlock.getFrom();
            if (from != null) {
                return complieQuery(defaultSchemaName, templateSql, id, sqlStatement);
            }
        }
        MetadataManager.INSTANCE.resolveMetadata(sqlStatement);
        int variantRefCount = getVariantRefCount(sqlStatement);
        BiFunction<String, String, Iterator> function = null;
        if (sqlStatement instanceof MySqlInsertStatement) {
            function = insertHandler(defaultSchemaName);
        }
        if (sqlStatement instanceof MySqlUpdateStatement || sqlStatement instanceof MySqlDeleteStatement) {
            function = updateHandler(defaultSchemaName);
        }
        if (function != null) {
            function = updateHandler(defaultSchemaName);
        }
        return getMycatPrepareObject(defaultSchemaName, templateSql, id, sqlStatement, variantRefCount, function);
    }

    public List<MycatSQLPrepareObject> query(MycatCalciteDataContext dataContext, String sql) {
        List<SQLStatement> statements = SQLUtils.parseStatements(sql, DbType.mysql);
        List<MycatSQLPrepareObject> collect = new ArrayList<>();
        for (SQLStatement statement : statements) {
            MycatSQLPrepareObject query = query(dataContext, sql, statement);
            if (query != null) {
                collect.add(query);
            } else {
                return Collections.emptyList();
            }
        }
        return collect;
    }

    private MycatSQLPrepareObject query(MycatCalciteDataContext dataContext, String sql, SQLStatement sqlStatement) {
        String defaultSchemaName = dataContext.getDefaultSchemaName();
        boolean ddl = sqlStatement instanceof SQLSelectStatement || sqlStatement instanceof MySqlInsertStatement
                || sqlStatement instanceof MySqlUpdateStatement || sqlStatement instanceof MySqlDeleteStatement;
        if (ddl) {
            return complieQuery(dataContext.getDefaultSchemaName(), sql, null, sqlStatement);
        }
        if (sqlStatement instanceof SQLCommitStatement) return commit(sql, defaultSchemaName);
        if (sqlStatement instanceof SQLRollbackStatement) return (rollback(sql, defaultSchemaName));
        if (sqlStatement instanceof SQLSetStatement) {
            for (SQLAssignItem item : ((SQLSetStatement) sqlStatement).getItems()) {
                SQLExpr target = item.getTarget();
                SQLExpr value = item.getValue();
            }
        }
        if (sqlStatement instanceof SQLUseStatement) {
            String normalize = SQLUtils.normalize(((SQLUseStatement) sqlStatement).getDatabase().getSimpleName());
            return use(sql, defaultSchemaName, normalize);
        }
        if (sqlStatement instanceof MySqlExplainStatement) {
            return explain(sql, (MySqlExplainStatement) sqlStatement, defaultSchemaName);
        }
        return null;
    }

    @NotNull
    private MycatSQLPrepareObject use(String sql, String defaultSchemaName, String normalize) {
        return new PlanRunnerImpl(defaultSchemaName, sql) {

            @Override
            public void innerEun(MycatCalciteDataContext dataContext) {
                dataContext.useSchema(normalize);
            }

            @Override
            public String innerExplain() {
                return "use " + normalize;
            }
        };
    }

    @NotNull
    private MycatSQLPrepareObject rollback(String sql, String defaultSchemaName) {
        return new PlanRunnerImpl(defaultSchemaName, sql) {

            @Override
            public void innerEun(MycatCalciteDataContext dataContext) {
                dataContext.rollback();
            }

            @Override
            public String innerExplain() {
                return "rollback";
            }
        };
    }

    @NotNull
    private MycatSQLPrepareObject commit(String sql, String defaultSchemaName) {
        return new PlanRunnerImpl(defaultSchemaName, sql) {

            @Override
            public void innerEun(MycatCalciteDataContext dataContext) {
                dataContext.commit();
            }

            @Override
            public String innerExplain() {
                return "commit";
            }
        };
    }

    @NotNull
    private MycatSQLPrepareObject explain(String sql, MySqlExplainStatement sqlStatement, String defaultSchemaName) {
        return new PlanRunnerImpl(defaultSchemaName, sql) {
            @Override
            public List<String> explain() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void innerEun(MycatCalciteDataContext dataContext) {
                throw new UnsupportedOperationException();
            }

            @Override
            public String innerExplain() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Supplier<RowBaseIterator> run(MycatCalciteDataContext dataContext) {
                return () -> {
                    SQLStatement statement = sqlStatement.getStatement();
                    MycatPrepareObject prepare = prepare(defaultSchemaName, statement.toString());
                    ResultSetBuilderImpl resultSetBuilder = ResultSetBuilderImpl.create();
                    List<String> explain = prepare.plan(Collections.emptyList()).explain();
                    resultSetBuilder.addColumnInfo("plan", Types.VARCHAR);
                    for (String s : explain) {
                        resultSetBuilder.addObjectRowPayload(new Object[]{s});
                    }
                    return resultSetBuilder.build();
                };
            }
        };
    }


    @NotNull
    private BiFunction<String, String, Iterator> insertHandler(String defaultSchemaName) {
        return (s, s2) -> MetadataManager.INSTANCE.getInsertInfoMap(defaultSchemaName, s2).entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
    }

    @NotNull
    private BiFunction<String, String, Iterator> updateHandler(String defaultSchemaName) {
        return (s, s2) -> MetadataManager.INSTANCE.rewriteSQL(defaultSchemaName, s2).entrySet().stream().map(i -> TextUpdateInfo.create(i.getKey(), i.getValue())).iterator();
    }

    @NotNull
    private MycatSQLPrepareObject getMycatPrepareObject(
            String defaultSchemaName,
            String templateSql,
            Long id,
            SQLStatement sqlStatement,
            int variantRefCount,
            BiFunction<String, String, Iterator> accept) {
        return new MycatDelegateSQLPrepareObject(defaultSchemaName, templateSql, new MycatTextUpdatePrepareObject(id, variantRefCount, (prepareObject, params) -> {
            StringBuilder out = new StringBuilder();
            SQLASTOutputVisitor visitor = SQLUtils.createOutputVisitor(out, DbType.mysql);
            visitor.setInputParameters(params);
            sqlStatement.accept(visitor);
            String sql = out.toString();
            return accept.apply(defaultSchemaName, sql);
        }));

    }


    public void close(Long id) {
        MycatPrepareObject mycatCalcitePrepare = PREPARE_MAP.remove(id);
        if (mycatCalcitePrepare != null) {
            close(mycatCalcitePrepare);
        }
    }

    private synchronized void close(MycatPrepareObject prepare) {
        if (prepare instanceof MycatSQLPrepareObject) {
            SQL_PREPASRE.remove(((MycatSQLPrepareObject) prepare).getSql());
        }
        PREPARE_MAP.remove(prepare.getId());
    }

    @NotNull
    private MycatSQLPrepareObject complieQuery(String defaultSchemaName, String sql, Long id, SQLStatement
            sqlStatement) {
        SQLSelectQueryBlock queryBlock = ((SQLSelectStatement) sqlStatement).getSelect().getQueryBlock();
        boolean forUpdate = queryBlock.isForUpdate();
        CalciteMySqlNodeVisitor calciteMySqlNodeVisitor = new CalciteMySqlNodeVisitor();
        sqlStatement.accept(calciteMySqlNodeVisitor);
        SqlNode sqlNode = calciteMySqlNodeVisitor.getSqlNode();
        MycatCalcitePlanner planner = MycatCalciteContext.INSTANCE.createPlanner(defaultSchemaName);
        SqlValidatorImpl sqlValidator = planner.getSqlValidator();
        MycatRowMetaData parameterRowType = null;
        if (id != null) {
            parameterRowType = new CalciteRowMetaData(sqlValidator.getParameterRowType(sqlNode).getFieldList());
        }
        MycatRowMetaData resultRowType = new CalciteRowMetaData(sqlValidator.getValidatedNodeType(sqlNode).getFieldList());
        return new MycatCalcitePrepare(id, defaultSchemaName, sql, sqlNode, parameterRowType, resultRowType, forUpdate);
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
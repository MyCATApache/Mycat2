package io.mycat.calcite.plan;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLReplaceable;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.expr.SQLExprUtils;
import com.alibaba.druid.sql.ast.expr.SQLVariantRefExpr;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.MycatDataContext;
import io.mycat.Response;
import io.mycat.beans.mycat.TransactionType;
import io.mycat.calcite.executor.MycatInsertExecutor;
import io.mycat.calcite.executor.MycatUpdateExecutor;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.physical.MycatInsertRel;
import io.mycat.calcite.physical.MycatUpdateRel;
import io.mycat.calcite.resultset.EnumeratorRowIterator;
import io.mycat.calcite.spm.Plan;
import io.mycat.util.Pair;
import io.mycat.util.VertxUtil;
import io.vertx.core.impl.future.PromiseInternal;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.util.SqlString;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

import static io.mycat.DrdsRunner.getJdbcExecuter;

public class PlanImplementorImpl implements PlanImplementor {
    public PlanImplementorImpl(MycatDataContext context, List<Object> params, final Response response) {
        this.context = context;
        this.params = params;
        this.response = response;
    }

    private final MycatDataContext context;
    private List<Object> params;
    private final Response response;

    @Override
    public PromiseInternal<Void> execute(MycatUpdateRel mycatUpdateRel) {
        MycatUpdateExecutor updateExecutor;
        updateExecutor = MycatUpdateExecutor.create(mycatUpdateRel, context, params);

        if (this.context.getTransactionSession().transactionType() == TransactionType.PROXY_TRANSACTION_TYPE) {
            if (updateExecutor.isProxy()) {
                Pair<String, String> singleSql = updateExecutor.getSingleSql();
                return this.response.proxyUpdate(singleSql.getKey(), singleSql.getValue());
            }
        }

        updateExecutor.open();
        return this.response.sendOk(
                updateExecutor.getAffectedRow(),
                updateExecutor.getLastInsertId()
        );
    }

    @Override
    public PromiseInternal<Void> execute(MycatInsertRel logical) {
        MycatInsertExecutor insertExecutor = MycatInsertExecutor.create(context, Objects.requireNonNull(logical), params);
        if (this.context.getTransactionSession().transactionType() == TransactionType.PROXY_TRANSACTION_TYPE) {
            if (insertExecutor.isProxy()) {
                Pair<String, String> singleSql = insertExecutor.getSingleSql();
                return response.proxyUpdate(singleSql.getKey(), singleSql.getValue());
            }
        }

        insertExecutor.open();
        return response.sendOk(insertExecutor.getAffectedRow(), insertExecutor.getLastInsertId());
    }

    @Override
    public PromiseInternal<Void> execute(Plan plan) {
        if (context.getTransactionSession().transactionType() == TransactionType.PROXY_TRANSACTION_TYPE) {
            RelNode physical = plan.getPhysical();
            if (physical instanceof MycatView) {
                ImmutableMultimap<String, SqlString> expandToSql = ((MycatView) physical).expandToSql(plan.forUpdate(), params);
                if (expandToSql.size() == 1) {
                    Map.Entry<String, SqlString> entry = expandToSql.entries().iterator().next();
                    String key = entry.getKey();
                    SqlString value = entry.getValue();
                    return response.proxySelect(context.resolveDatasourceTargetName(key), apply(value.getSql(), params));
                }
            }
        }
        PromiseInternal<Void> newPromise = VertxUtil.newPromise();
        CompletableFuture<Enumerable<Object[]>> jdbcExecuter = getJdbcExecuter(plan, context, params);
        jdbcExecuter.handle((BiFunction<Enumerable<Object[]>, Throwable, Object>) (objects, throwable) -> {
            if (throwable != null) {
                newPromise.fail(throwable);
                return response.sendError(throwable);
            } else {
                newPromise.tryComplete();
                return response.sendResultSet(new EnumeratorRowIterator((plan.getMetaData()), objects.enumerator()));
            }
        });
        return newPromise;
    }


    public static String apply(String parameterizedSql, List<Object> parameters) {
        SQLStatement sqlStatement = SQLUtils.parseSingleMysqlStatement(parameterizedSql);
        sqlStatement.accept(new MySqlASTVisitorAdapter() {
            @Override
            public void endVisit(SQLVariantRefExpr x) {
                SQLReplaceable parent = (SQLReplaceable) x.getParent();
                parent.replace(x, SQLExprUtils.fromJavaObject(parameters.get(x.getIndex())));
            }
        });
        return sqlStatement.toString();
    }
}

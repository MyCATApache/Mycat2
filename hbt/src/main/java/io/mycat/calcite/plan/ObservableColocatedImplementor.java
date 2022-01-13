package io.mycat.calcite.plan;

import cn.mycat.vertx.xa.XaSqlConnection;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import com.google.common.collect.ImmutableMultimap;
import io.mycat.*;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.MycatRel;
import io.mycat.calcite.MycatRelDatasourceSourceInfo;
import io.mycat.calcite.executor.MycatPreparedStatementUtil;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.table.ShardingIndexTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.mycat.util.NameMap;
import io.vertx.core.Future;
import org.apache.calcite.sql.util.SqlString;
import org.jetbrains.annotations.Nullable;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ObservableColocatedImplementor extends ObservablePlanImplementorImpl {
    public ObservableColocatedImplementor(XaSqlConnection xaSqlConnection, MycatDataContext context, DrdsSqlWithParams drdsSqlWithParams, Response response) {
        super(xaSqlConnection, context, drdsSqlWithParams, response);
    }


    @Override
    public Future<Void> executeQuery(Plan plan) {
        Optional<ExplainDetail> singleViewOptional = ColocatedPlanner.executeQuery(this.context, plan, this.drdsSqlWithParams);
        if (singleViewOptional.isPresent()) {
            ExplainDetail explainDetail = singleViewOptional.get();
            return response.proxySelect(explainDetail.getTargets(), explainDetail.getSql(), explainDetail.getParams());
        }
        return super.executeQuery(plan);
    }
}

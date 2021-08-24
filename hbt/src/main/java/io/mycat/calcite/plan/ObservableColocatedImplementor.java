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
import io.mycat.calcite.spm.Plan;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.mycat.util.NameMap;
import io.vertx.core.Future;
import org.apache.calcite.sql.util.SqlString;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class ObservableColocatedImplementor extends ObservablePlanImplementorImpl {
    public ObservableColocatedImplementor(XaSqlConnection xaSqlConnection, MycatDataContext context, DrdsSqlWithParams drdsSqlWithParams, Response response) {
        super(xaSqlConnection, context, drdsSqlWithParams, response);
    }

    @Override
    public Future<Void> executeQuery(Plan plan) {
        MycatRel mycatRel = plan.getMycatRel();
        CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
        AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext = new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(context, codeExecuterContext, drdsSqlWithParams);

        if (mycatRel instanceof MycatView && !drdsSqlWithParams.getAliasList().isEmpty()) {//case 1
            MycatView mycatView = (MycatView) mycatRel;
            List<PartitionGroup> partitions = sqlMycatDataContext.getPartition(mycatRel.getDigest()).orElse(Collections.emptyList());
            if (partitions.size() == 1) {
                ImmutableMultimap<String, SqlString> sqlMap = mycatView
                        .apply(context.getMergeUnionSize(),codeExecuterContext.getRelContext().get(mycatView.getDigest()).getSqlTemplate(), partitions, drdsSqlWithParams.getParams());
                Map.Entry<String, SqlString> kv = sqlMap.entries().stream().iterator().next();
                SqlString sqlString = kv.getValue();
                ExplainDetail explainDetail = new ExplainDetail(ExecuteType.QUERY, Collections.singletonList(kv.getKey()), sqlString.getSql(), null,   MycatPreparedStatementUtil.extractParams(drdsSqlWithParams.getParams(),sqlString.getDynamicParameters()));
                return response.execute(explainDetail);
            }
        }
        Map<String, MycatRelDatasourceSourceInfo> relContext = codeExecuterContext.getRelContext();
        if (relContext.size()==1){//Colocated Push Down
            List<PartitionGroup> partitions = sqlMycatDataContext.getPartition(mycatRel.getDigest()).orElse(Collections.emptyList());
            if (partitions.size()==1){
                NameMap<Partition> partition = NameMap.immutableCopyOf((partitions.get(0).getMap()));
                String targetName = partition.values().iterator().next().getTargetName();
                SQLStatement parameterizedStatement = drdsSqlWithParams.getParameterizedStatement().clone();

                parameterizedStatement.accept(new MySqlASTVisitorAdapter(){
                    @Override
                    public boolean visit(SQLExprTableSource x) {
                        String schema = SQLUtils.normalize(x.getSchema());
                        String table = SQLUtils.normalize(x.getTableName());
                        String s = schema+ "_" +table ;
                        Partition tableInfo = partition.get(s,false);
                        MycatSQLExprTableSourceUtil.setSqlExprTableSource(tableInfo.getSchema(),tableInfo.getTable(),x);
                        return false;
                    }
                });
                ExplainDetail explainDetail = new ExplainDetail(ExecuteType.QUERY, Collections.singletonList(targetName), parameterizedStatement.toString(), null, drdsSqlWithParams.getParams());
                return response.execute(explainDetail);
            }
        }
        return super.executeQuery(plan);
    }
}

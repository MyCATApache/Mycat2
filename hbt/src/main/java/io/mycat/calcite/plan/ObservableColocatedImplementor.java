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


    class Replacer extends MySqlASTVisitorAdapter {
        NameMap<Partition> partition;
        boolean success = true;

        public Replacer(NameMap<Partition> partition) {
            this.partition = partition;
        }

        @Override
        public boolean visit(SQLExprTableSource x) {
            if (!success) return false;
            if (x.getAlias() == null) {
                x.setAlias(x.getTableName());
            }
            String schema = SQLUtils.normalize(x.getSchema());
            String table = SQLUtils.normalize(x.getTableName());
            String s = schema + "_" + table;
            Partition tableInfo = partition.get(s, false);
            if (tableInfo != null) {
                MycatSQLExprTableSourceUtil.setSqlExprTableSource(tableInfo.getSchema(), tableInfo.getTable(), x);
            } else {
                success = false;
            }
            return false;
        }
    }

    @Override
    public Future<Void> executeQuery(Plan plan) {
        Optional<PartitionGroup> colocatedPushDownOptional = checkColocatedPushDown(plan);
        if (colocatedPushDownOptional.isPresent()) {
            PartitionGroup partitionGroup = colocatedPushDownOptional.get();
            NameMap<Partition> partition = NameMap.immutableCopyOf(partitionGroup.getMap());
            String targetName = partitionGroup.getTargetName();
            SQLStatement parameterizedStatement = drdsSqlWithParams.getParameterizedStatement().clone();
            Replacer replacer = new Replacer(partition);
            parameterizedStatement.accept(replacer);
            if (replacer.success) {
                ExplainDetail explainDetail = new ExplainDetail(ExecuteType.QUERY, Collections.singletonList(targetName), parameterizedStatement.toString(), null, drdsSqlWithParams.getParams());
                return response.execute(explainDetail);
            }
        }
        return super.executeQuery(plan);
    }

    private Optional<PartitionGroup> checkColocatedPushDown(Plan plan) {
        CodeExecuterContext codeExecuterContext = plan.getCodeExecuterContext();
        AsyncMycatDataContextImpl.SqlMycatDataContextImpl sqlMycatDataContext = new AsyncMycatDataContextImpl.SqlMycatDataContextImpl(context, codeExecuterContext, drdsSqlWithParams);
        Map<String, MycatRelDatasourceSourceInfo> relContext = codeExecuterContext.getRelContext();
        List<List<PartitionGroup>> lists = new ArrayList<>();
        for (MycatRelDatasourceSourceInfo mycatRelDatasourceSourceInfo : relContext.values()) {
            if (mycatRelDatasourceSourceInfo.getRelNode() instanceof MycatView) {
                MycatView mycatView = (MycatView) mycatRelDatasourceSourceInfo.getRelNode();
                if (mycatView.getDistribution().type() == Distribution.Type.SHARDING) {
                    for (ShardingTable shardingTable : mycatView.getDistribution().getShardingTables()) {
                        if (shardingTable instanceof ShardingIndexTable) {
                            return Optional.empty();
                        }
                    }
                }
                lists.add(sqlMycatDataContext.getPartition(mycatView.getDigest()).orElse(Collections.emptyList()));
            } else {
                return Optional.empty();
            }
        }

        PartitionGroup result = null;//Colocated Push Down
        for (List<PartitionGroup> list : lists) {
            if (list.size() == 1) {
                PartitionGroup each = list.get(0);
                if (result == null) {
                    result = new PartitionGroup(each.getTargetName(), new HashMap<>(each.getMap()));
                } else if (result.getTargetName().equals(each.getTargetName())) {
                    Map<String, Partition> eachMap = each.getMap();
                    Map<String, Partition> resMap = result.getMap();
                    for (Map.Entry<String, Partition> entry : eachMap.entrySet()) {
                        if (resMap.containsKey(entry.getKey())) {
                            //已经存在的分区不一致,所以不匹配
                            Partition existed = resMap.get(entry.getKey());
                            if (!existed.getUniqueName().equals(entry.getValue().getUniqueName())) {
                                return Optional.empty();
                            }
                        } else {
                            resMap.put(entry.getKey(), entry.getValue());
                        }
                    }
                } else {
                    return Optional.empty();
                }
            } else {
                return Optional.empty();
            }
        }
        return Optional.ofNullable(result);
    }
}

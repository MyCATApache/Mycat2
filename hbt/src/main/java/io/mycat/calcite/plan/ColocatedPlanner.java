package io.mycat.calcite.plan;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLStatement;
import com.alibaba.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.druid.sql.dialect.mysql.visitor.MySqlASTVisitorAdapter;
import io.mycat.*;
import io.mycat.calcite.CodeExecuterContext;
import io.mycat.calcite.MycatRelDatasourceSourceInfo;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.spm.Plan;
import io.mycat.calcite.table.ShardingIndexTable;
import io.mycat.calcite.table.ShardingTable;
import io.mycat.util.MycatSQLExprTableSourceUtil;
import io.mycat.util.NameMap;

import java.util.*;

public class ColocatedPlanner {

    static class Replacer extends MySqlASTVisitorAdapter {
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

    public static Optional<PartitionGroup> checkColocatedPushDown(MycatDataContext context,Plan plan,DrdsSqlWithParams drdsSqlWithParams) {
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

    public static Optional<ExplainDetail> executeQuery(MycatDataContext dataContext,Plan plan,DrdsSqlWithParams drdsSqlWithParams) {
        Optional<PartitionGroup> colocatedPushDownOptional = checkColocatedPushDown(dataContext,plan,drdsSqlWithParams);
        if (colocatedPushDownOptional.isPresent()) {
            PartitionGroup partitionGroup = colocatedPushDownOptional.get();
            NameMap<Partition> partition = NameMap.immutableCopyOf(partitionGroup.getMap());
            String targetName = partitionGroup.getTargetName();
            SQLStatement parameterizedStatement = drdsSqlWithParams.getParameterizedStatement().clone();
            Replacer replacer = new Replacer(partition);
            parameterizedStatement.accept(replacer);
            if (replacer.success) {
                ExplainDetail explainDetail = new ExplainDetail(ExecuteType.QUERY, Collections.singletonList(targetName), parameterizedStatement.toString(), null, drdsSqlWithParams.getParams());
                return Optional.of(explainDetail);
            }
        }
        return Optional.empty();
    }
}

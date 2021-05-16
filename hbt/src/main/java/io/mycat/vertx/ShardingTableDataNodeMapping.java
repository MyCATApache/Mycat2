package io.mycat.vertx;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import io.mycat.DataNode;
import io.mycat.DrdsSqlWithParams;
import io.mycat.MetaClusterCurrent;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.MycatHint;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.logical.MycatViewDataNodeMapping;
import io.mycat.calcite.logical.MycatViewSqlString;
import io.mycat.calcite.rewriter.Distribution;
import io.mycat.calcite.table.GlobalTable;
import io.mycat.config.ServerConfig;
import io.mycat.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlString;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@NoArgsConstructor

public class ShardingTableDataNodeMapping implements DataNodeMapping {
    MycatViewDataNodeMapping mycatViewDataNodeMapping;
    SqlNode sqlTemplate;

    public ShardingTableDataNodeMapping(MycatViewDataNodeMapping mycatViewDataNodeMapping, SqlNode sqlTemplate) {
        this.mycatViewDataNodeMapping = mycatViewDataNodeMapping;
        this.sqlTemplate = sqlTemplate;
    }

    @Override
    public MycatViewSqlString apply(DrdsSqlWithParams drdsSqlWithParams) {
        int mergeUnionSize = MetaClusterCurrent.exist(ServerConfig.class) ? MetaClusterCurrent.wrapper(ServerConfig.class).getMergeUnionSize() : 5;
        return apply(mycatViewDataNodeMapping, sqlTemplate, drdsSqlWithParams, mergeUnionSize);
    }

    /**
     * ImmutableMultimap<String, SQLSelectStatement>
     *
     * @param mycatViewDataNodeMapping
     * @param sqlTemplateArg
     * @param mergeUnionSize
     * @return
     */
    public static MycatViewSqlString apply(MycatViewDataNodeMapping mycatViewDataNodeMapping,
                                           SqlNode sqlTemplateArg,
                                           DrdsSqlWithParams drdsSqlWithParams,
                                           int mergeUnionSize) {
        SqlNode sqlTemplate = sqlTemplateArg;
        for (MycatHint mycatHint : Optional.ofNullable(drdsSqlWithParams.getHints()).orElse(Collections.emptyList())) {
            for (MycatHint.Function function : mycatHint.getFunctions()) {
               if("SCAN".equals(function.getName())){
                   List<MycatHint.Argument> arguments = function.getArguments();
                   break;
               }
            }

        }

        Stream<Map<String, DataNode>> dataNodes = mycatViewDataNodeMapping.apply(drdsSqlWithParams.getParams());
        if (mycatViewDataNodeMapping.getType() == Distribution.Type.BroadCast) {
            GlobalTable globalTable = mycatViewDataNodeMapping.distribution().getGlobalTables().get(0);
            List<DataNode> globalDataNode = globalTable.getGlobalDataNode();
            int i = ThreadLocalRandom.current().nextInt(0, globalDataNode.size());
            DataNode dataNode = globalDataNode.get(i);
            String targetName = dataNode.getTargetName();
            Map<String, DataNode> nodeMap = dataNodes.findFirst().get();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
            SqlNode sqlSelectStatement = MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate, drdsSqlWithParams.getParams(), nodeMap);
            return new MycatViewSqlString(ImmutableMultimap.of(targetName, sqlSelectStatement.toSqlString(dialect)));
        }
        if (mergeUnionSize == 0 || mycatViewDataNodeMapping.containsOrder()) {
            ImmutableMultimap.Builder<String, SqlString> builder = ImmutableMultimap.builder();
            dataNodes.forEach(m -> {
                String targetName = m.values().iterator().next().getTargetName();
                SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
                SqlString sqlString = MycatCalciteSupport.toSqlString(MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate, drdsSqlWithParams.getParams(), m), (dialect));
                builder.put(targetName, sqlString);
            });
            return new MycatViewSqlString(builder.build());
        }
        Map<String, List<Map<String, DataNode>>> collect = dataNodes.collect(Collectors.groupingBy(m -> m.values().iterator().next().getTargetName()));
        ImmutableMultimap.Builder<String, SqlString> resMapBuilder = ImmutableMultimap.builder();
        for (Map.Entry<String, List<Map<String, DataNode>>> entry : collect.entrySet()) {
            String targetName = entry.getKey();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(targetName);
            Iterator<List<Map<String, DataNode>>> iterator = Iterables.partition(entry.getValue(), mergeUnionSize + 1).iterator();
            while (iterator.hasNext()) {
                List<Map<String, DataNode>> eachList = iterator.next();
                ImmutableList.Builder<SqlString> builderList = ImmutableList.builder();
                SqlString string = null;
                List<Integer> list = new ArrayList<>();
                for (Map<String, DataNode> each : eachList) {
                    string = MycatCalciteSupport.toSqlString(MycatCalciteSupport.INSTANCE.sqlTemplateApply(sqlTemplate, drdsSqlWithParams.getParams(), each), dialect);
                    if (string.getDynamicParameters() != null) {
                        list.addAll(string.getDynamicParameters());
                    }
                    builderList.add(string);
                }
                ImmutableList<SqlString> relNodes = builderList.build();
                resMapBuilder.put(targetName,
                        new SqlString(dialect,
                                relNodes.stream().map(i -> i.getSql()).collect(Collectors.joining(" union all ")),
                                ImmutableList.copyOf(list)));
            }
        }
        return new MycatViewSqlString(resMapBuilder.build());
    }
}

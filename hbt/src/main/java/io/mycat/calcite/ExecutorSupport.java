package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Iterables;
import io.mycat.DataNode;
import io.mycat.MetaClusterCurrent;
import io.mycat.MetadataManager;
import io.mycat.config.ServerConfig;
import io.mycat.replica.ReplicaSelectorManager;
import io.mycat.replica.ReplicaSelectorRuntime;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExecutorSupport {

  public static    Map<String, List<SqlString>> physicalSqlMerge(Map<String, List<SqlString>> sqls, boolean containsOrder) {
        ServerConfig serverConfig = MetaClusterCurrent.wrapper(ServerConfig.class);
        if (serverConfig.getMergeUnionSize() == 0) {
            return sqls;
        }
        return physicalSqlMerge(sqls, containsOrder);
    }

   public static Map<String, List<SqlString>> physicalSqlMerge(Map<String, List<SqlString>> sqls, int unionAllCount, boolean containsOrder) {
        if (containsOrder) {
            return sqls;
        }
        ImmutableMap.Builder<String, List<SqlString>> resMapBuilder = ImmutableMap.builder();
        int pSize = unionAllCount + 1;
        for (Map.Entry<String, List<SqlString>> stringListEntry : sqls.entrySet()) {
            Iterator<List<SqlString>> iterator = Iterables.partition(stringListEntry.getValue(), pSize).iterator();
            SqlDialect dialect = MycatCalciteSupport.INSTANCE.getSqlDialectByTargetName(stringListEntry.getKey());
            ImmutableList.Builder<SqlString> builder = ImmutableList.builder();
            while (iterator.hasNext()) {
                List<SqlString> eachList = iterator.next();
                ImmutableList.Builder<SqlString> builderList = ImmutableList.builder();
                List<Integer> list = new ArrayList<>();
                for (SqlString each : eachList) {
                    SqlString string = each;
                    if (string.getDynamicParameters() != null) {
                        list.addAll(string.getDynamicParameters());
                    }
                    builderList.add(string);
                }
                ImmutableList<SqlString> relNodes = builderList.build();
                builder.add(
                        new SqlString(dialect,
                                relNodes.stream().map(i -> i.getSql()).collect(Collectors.joining(" union all ")),
                                ImmutableList.copyOf(list)));
            }
            resMapBuilder.put(stringListEntry.getKey(), builder.build());
        }
        return resMapBuilder.build();
    }

}

package io.mycat.route;

import com.alibaba.fastsql.sql.ast.SQLStatement;
import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.alibaba.fastsql.sql.dialect.mysql.ast.statement.MySqlSelectQueryBlock;
import io.mycat.hbt.ast.base.Schema;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.ShardingTable;
import io.mycat.TableHandler;

import java.util.List;
import java.util.Map;

public class SimpleShardingRouter implements SqlRouteChain {
    @Override
    public boolean handle(ParseContext parseContext) {
        List<SQLExprTableSource> leftTables = parseContext.startAndGetLeftTables();
        if (leftTables == null || leftTables.isEmpty() || parseContext.isNoAgg() || parseContext.isNoComplexDatasource() || parseContext.isNoSubQuery() || parseContext.isNoOrder()) {
            return false;
        }
        if (leftTables.size() == 1) {
            SQLExprTableSource tableSource = leftTables.get(0);
            MySqlSelectQueryBlock queryBlock = parseContext.tryGetQueryBlock();
            boolean distinct = queryBlock.isDistinct();
            TableHandler table = MetadataManager.INSTANCE.getTable(tableSource.getSchema(), tableSource.getTableName());
            if (table instanceof ShardingTable) {
                SQLStatement sqlStatement = parseContext.getSqlStatement();
                Map<String, List<String>> stringListMap = MetadataManager.INSTANCE.rewriteSQL(parseContext.getDefaultSchema(), sqlStatement.toString());
                int size = stringListMap.size();
                if (size == 0) {
                    return false;
                } else {
                    HBTBuilder hbtBuilder = HBTBuilder.create();
                    Map.Entry<String, List<String>> next = stringListMap.entrySet().iterator().next();
                    String key = next.getKey();
                    List<String> values = next.getValue();
                    for (String value : values) {
                        hbtBuilder.from(key, value);
                    }
                    HBTBuilder union = hbtBuilder.unionMore(!distinct);
                    Schema build = hbtBuilder.build();
                }
            }
        }

        return false;
    }
}
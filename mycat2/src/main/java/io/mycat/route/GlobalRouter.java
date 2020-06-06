package io.mycat.route;

import com.alibaba.fastsql.sql.ast.statement.SQLExprTableSource;
import com.google.common.collect.Sets;
import io.mycat.DataNode;

import java.util.*;
import java.util.stream.Collectors;

public class GlobalRouter implements SqlRouteChain {

    @Override
    public boolean handle(ParseContext parseContext) {
        List<SQLExprTableSource> leftTables = parseContext.startAndGetLeftTables();
        List<Set<String>> set = new ArrayList<>();
        HashMap<SQLExprTableSource, Set<DataNode>> mapping = new HashMap<>();
        for (SQLExprTableSource leftTable : leftTables) {
            Set<DataNode> globalRanges = parseContext.getGlobalRange(leftTable);
            if (globalRanges == null) {
                return false;
            }
            mapping.put(leftTable, globalRanges);
            set.add(globalRanges.stream().map(i->i.getTargetName()).collect(Collectors.toSet()));
        }
        //求交集
        Set<String> dataNodes = set.stream().reduce((dataNodes1, dataNodes2) -> {
            return Sets.intersection(dataNodes1, dataNodes2);
        }).orElse(Collections.emptySet());
        if (dataNodes.size() != 1) return false;
        String targetName = dataNodes.iterator().next();
        for (Map.Entry<SQLExprTableSource, Set<DataNode>> entry : mapping.entrySet()) {
            SQLExprTableSource key = entry.getKey();
            Set<DataNode> value = entry.getValue();
            boolean change = false;
            for (DataNode dataNode : value) {
               if( targetName.equals( dataNode.getTargetName())){
                   change = true;
                   parseContext.changeSchemaTable(key, dataNode);
                   break;
               }
            }
            if (!change){
                return false;
            }
        }
        String sql = parseContext.getSqlStatement().toString();
        parseContext.plan(HBTBuilder.create()
                .from(targetName,sql)
                .build());
        return true;
    }
}
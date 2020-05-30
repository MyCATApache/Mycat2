package io.mycat.mpp;

import com.alibaba.fastsql.sql.ast.SQLObject;
import com.alibaba.fastsql.sql.ast.expr.SQLAggregateExpr;
import io.mycat.mpp.plan.DualPlan;
import io.mycat.mpp.plan.LogicTablePlan;

import java.util.*;

public class TranslatorBlackboard {
    final List<LogicTablePlan> tables = new ArrayList<>();
    final Map<String,LogicTablePlan> tableMap = new HashMap<>();
    final Map<SQLAggregateExpr,SqlValue> aggMap = new HashMap<>();
    final Map<Object,Object> astMap = new HashMap<>();
    public TranslatorBlackboard() {
        tables.add(new DualPlan());
    }

    public Optional<LogicTablePlan> lookUp(String schemaName, String tableName, String columnName) {
        if ("dual".equals(tableName)) {
            return Optional.of(new DualPlan());
        }
        LogicTablePlan plan = tableMap.get(tableName);
        if (plan!=null){
            return Optional.of(plan);
        }
        return tables.stream().filter(logicTablePlan -> {
            if (Objects.equals(schemaName, logicTablePlan.getSchemaName())) {
                if (logicTablePlan.getTableName().equals(tableName)) {
                    return logicTablePlan.getRowType().contains(columnName);
                }
            }
            return false;
        }).findAny();
    }

    public void addTable(String s, LogicTablePlan from) {
        tables.add(from);
        tableMap.put(s,from);
    }

    public void collect(SQLAggregateExpr x, SqlValue result) {
        aggMap.put(x,result);
    }

    public Map<SQLAggregateExpr, SqlValue> getAggMap() {
        return aggMap;
    }

    public SqlValue collect(SQLObject ast,SqlValue r){
        astMap.put(ast,r);
        return r;
    }

}
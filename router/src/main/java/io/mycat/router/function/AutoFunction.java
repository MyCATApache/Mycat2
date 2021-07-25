/**
 * Copyright (C) <2021>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router.function;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import io.mycat.Partition;
import io.mycat.RangeVariable;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;

import java.text.MessageFormat;
import java.util.*;
import java.util.function.ToIntFunction;

public abstract class AutoFunction extends CustomRuleFunction {
    String name;
    int dbNum;
    int tableNum;
    Object dbMethod;
    Object tableMethod;
    private Set<String> dbKeys;
    private Set<String> tableKeys;
    final ToIntFunction<Object> finalDbFunction;
    final ToIntFunction<Object> finalTableFunction;

    public AutoFunction(int dbNum,
                        int tableNum,
                        SQLMethodInvokeExpr dbMethod,
                        SQLMethodInvokeExpr tableMethod,
                        Set<String> dbKeys,
                        Set<String> tableKeys,
                        final ToIntFunction<Object> finalDbFunction,
                        final ToIntFunction<Object> finalTableFunction,
                        int storeNum) {
        this.dbNum = dbNum;
        this.tableNum = tableNum;
        this.dbMethod = dbMethod;
        this.tableMethod = tableMethod;
        this.dbKeys = dbKeys;
        this.tableKeys = tableKeys;
        this.finalDbFunction = finalDbFunction;
        this.finalTableFunction = finalTableFunction;

        this.name = MessageFormat.format("dbNum:{0} tableNum:{1} dbMethod:{2} tableMethod:{3} storeNum:{4}",
                dbNum, tableNum, extractKey(dbMethod), extractKey(tableMethod), storeNum);
    }

    private static String extractKey(SQLMethodInvokeExpr method) {
        if (method == null) {
            return "null";
        }
        String methodName = method.getMethodName().toUpperCase();
        //DD,MM,MMDD,MOD_HASH,UNI_HASH,WEEK,YYYYDD,YYYYMM,YYYYWEEK
        String key;
        switch (methodName) {
            case "DD":
            case "MM":
            case "MMDD":
            case "MOD_HASH":
            case "UNI_HASH":
            case "WEEK":
            case "YYYYDD":
            case "YYYYMM":
            case "YYYYWEEK":
                key = methodName;
                break;
            case "RANGE_HASH": {
                List<SQLExpr> arguments = method.getArguments();
                SQLExpr sqlExpr = arguments.get(2);
                key = "RANGE_HASH$" + sqlExpr;
                break;
            }
            case "RIGHT_SHIFT": {
                List<SQLExpr> arguments = method.getArguments();
                SQLExpr sqlExpr = arguments.get(1);
                key = "RIGHT_SHIFT" + sqlExpr;
                break;
            }
            default:
                key = method.toString();
        }
        return key;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<Partition> calculate(Map<String, RangeVariable> values) {
        boolean getDbIndex = false;
        int dIndex = 0;

        boolean getTIndex = false;
        int tIndex = 0;

        Set<Map.Entry<String, RangeVariable>> entries = values.entrySet();
        for (Map.Entry<String, RangeVariable> e : entries) {
            RangeVariable rangeVariable = e.getValue();
            for (String dbShardingKey : dbKeys) {
                if (SQLUtils.nameEquals(dbShardingKey, e.getKey())) {
                    switch (rangeVariable.getOperator()) {
                        case EQUAL:
                            Object value = rangeVariable.getValue();
                            dIndex = finalDbFunction.applyAsInt(value);
                            getDbIndex = true;
                            if (dIndex < 0) {
                                finalDbFunction.applyAsInt(value);
                                throw new IllegalArgumentException();
                            }
                            break;
                        case RANGE:
                        default:
                            continue;
                    }
                }
            }
            for (String tableShardingKey : tableKeys) {
                if (SQLUtils.nameEquals(tableShardingKey, e.getKey())) {
                    switch (rangeVariable.getOperator()) {
                        case EQUAL:
                            Object value = rangeVariable.getValue();
                            tIndex = finalTableFunction.applyAsInt(value);
                            getTIndex = true;
                            break;
                        case RANGE:
                        default:
                            continue;
                    }
                }
            }
        }
        if (getDbIndex && getTIndex) {
            return (List) scanOnlyDbTableIndex(dIndex, tIndex);
        }
        if (getTIndex) {
            return (List) scanOnlyTableIndex(tIndex);
        }
        if (getDbIndex) {
            return (List) scanOnlyDbIndex(dIndex);
        }
        return (List) scanAll();
    }

    public abstract List<IndexDataNode> scanAll();

    public abstract List<IndexDataNode> scanOnlyTableIndex(int index);

    public abstract List<IndexDataNode> scanOnlyDbIndex(int index);

    public abstract List<IndexDataNode> scanOnlyDbTableIndex(int dbIndex, int tableIndex);

    @Override
    protected void init(ShardingTableHandler tableHandler, Map<String, Object> properties, Map<String, Object> ranges) {

    }

    @Override
    public boolean isShardingKey(String name) {
        name = SQLUtils.normalize(name);
        return dbKeys.contains(name) || tableKeys.contains(name);
    }

    @Override
    public boolean isShardingDbKey(String name) {
        name = SQLUtils.normalize(name);
        return dbKeys.contains(name);
    }

    @Override
    public boolean isShardingTableKey(String name) {
        name = SQLUtils.normalize(name);
        return tableKeys.contains(name);
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (AutoFunction.class.isAssignableFrom(customRuleFunction.getClass())) {
            AutoFunction ruleFunction = (AutoFunction) customRuleFunction;
            return Objects.equals(this.name, ruleFunction.name);
        }
        return false;
    }

    @Override
    public String getErUniqueID() {
        return name;
    }
}

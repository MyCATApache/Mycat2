/**
 * Copyright (C) <2021>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.router.function;

import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.SQLMethodInvokeExpr;
import io.mycat.DataNode;
import io.mycat.RangeVariable;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;

import java.text.MessageFormat;
import java.util.*;
import java.util.function.Function;

public class AutoFunction extends CustomRuleFunction {
    String name;
    int dbNum;
    int tableNum;
    Object dbMethod;
    Object tableMethod;
    private Set<String> dbKeys;
    private Set<String> tableKeys;
    Function<Map<String, Collection<RangeVariable>>, List<DataNode>> function;

    public AutoFunction(int dbNum,
                        int tableNum,
                        SQLMethodInvokeExpr dbMethod,
                        SQLMethodInvokeExpr tableMethod,
                        Set<String> dbKeys,
                        Set<String> tableKeys,
                        Function<Map<String, Collection<RangeVariable>>, List<DataNode>> function, String erUniqueName) {
        this.dbNum = dbNum;
        this.tableNum = tableNum;
        this.dbMethod = dbMethod;
        this.tableMethod = tableMethod;
        this.dbKeys = dbKeys;
        this.tableKeys = tableKeys;
        this.function = function;

        this.name = MessageFormat.format("dbNum:{0} tableNum:{1} dbMethod:{2} tableMethod:{3}",
                dbNum, tableNum, exractKey(dbMethod), exractKey(tableMethod));
    }

    private  static String exractKey(SQLMethodInvokeExpr method) {
        if (method == null){
            return "null";
        }
        String methodName = method.getMethodName().toUpperCase();
        //DD,MM,MMDD,MOD_HASH,UNI_HASH,WEEK,YYYYDD,YYYYMM,YYYYWEEK
        String key ;
        switch (methodName){
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
            case "RANGE_HASH":{
                List<SQLExpr> arguments = method.getArguments();
                SQLExpr sqlExpr = arguments.get(2);
                key="RANGE_HASH$"+sqlExpr;
                break;
            }
            case "RIGHT_SHIFT":{
                List<SQLExpr> arguments = method.getArguments();
                SQLExpr sqlExpr = arguments.get(1);
                key="RIGHT_SHIFT"+sqlExpr;
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
    public List<DataNode> calculate(Map<String, Collection<RangeVariable>> values) {
        return Objects.requireNonNull(function.apply(values));
    }

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

package io.mycat.router.function;

import com.alibaba.druid.sql.SQLUtils;
import io.mycat.DataNode;
import io.mycat.RangeVariable;
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;

import java.text.MessageFormat;
import java.util.*;
import java.util.function.Function;

public class AutoFunction extends CustomRuleFunction {
    int dbNum;
    int tableNum;
    Object dbMethod;
    Object tableMethod;
    Set<String> keys;
    Function<Map<String, Collection<RangeVariable>>, List<DataNode>> function;

    public AutoFunction(int dbNum,
                        int tableNum,
                        Object dbMethod,
                        Object tableMethod,
                        Set<String> keys,
                        Function<Map<String, Collection<RangeVariable>>, List<DataNode>> function) {
        this.dbNum = dbNum;
        this.tableNum = tableNum;
        this.dbMethod = dbMethod;
        this.tableMethod = tableMethod;
        this.keys = keys;
        this.function = function;
    }

    @Override
    public String name() {
        return MessageFormat.format("dbNum:{0} tableNum:{1} dbMethod:{2} tableMethod:{3}",
                dbNum, tableNum, dbMethod, tableMethod);
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
        return keys.contains(SQLUtils.normalize(name));
    }

    @Override
    public boolean isSameDistribution(CustomRuleFunction customRuleFunction) {
        if (customRuleFunction == null) return false;
        if (AutoFunction.class.isAssignableFrom(customRuleFunction.getClass())) {
            AutoFunction ruleFunction = (AutoFunction) customRuleFunction;
            int dbNum = ruleFunction.dbNum;
            int tableNum = ruleFunction.tableNum;
            Object dbMethod = ruleFunction.dbMethod;
            Object tableMethod = ruleFunction.tableMethod;
            return Objects.equals(this.dbNum, dbNum) &&
                    Objects.equals(this.tableNum, tableNum) &&
                    Objects.equals(this.dbMethod, dbMethod) &&
                    Objects.equals(this.tableMethod, tableMethod);
        }
        return false;
    }
}

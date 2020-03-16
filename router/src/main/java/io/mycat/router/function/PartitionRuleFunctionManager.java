/**
 * Copyright (C) <2020>  <mycat>
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

import io.mycat.config.SharingFuntionRootConfig;
import io.mycat.router.RuleFunction;

import java.util.Collections;
import java.util.Map;

public enum PartitionRuleFunctionManager {
    INSTANCE;

    public static RuleFunction createFunction(String name, String clazz)
            throws Exception {
        Class<?> clz = Class.forName(clazz);
        //判断是否继承AbstractPartitionAlgorithm
        if (!RuleFunction.class.isAssignableFrom(clz)) {
            throw new IllegalArgumentException("rule function must implements "
                    + RuleFunction.class.getName() + ", name=" + name);
        }
        return (RuleFunction) clz.getDeclaredConstructor().newInstance();
    }
    public static ColumnJoinerRuleFunction createColumnJoinerRuleFunction(String name, String clazz) throws Exception {
        return new ColumnJoinerRuleFunction(name,createFunction(name,clazz));
    }

    public static RuleFunction getRuleAlgorithm(SharingFuntionRootConfig.ShardingFuntion funtion)
            throws Exception {
        Map<String, String> properties = funtion.getProperties();
        properties = (properties == null) ? Collections.emptyMap() : properties;
        funtion.setProperties(properties);
        RuleFunction rootFunction = createFunction(funtion.getName(), funtion.getClazz());
        rootFunction.callInit(funtion.getProperties(), funtion.getRanges());
        return rootFunction;
    }

    public RuleFunction getRuleAlgorithm(String name, String clazz, Map<String, String> properties, Map<String, String> ranges) {
        try {
            RuleFunction function = createFunction(name, clazz);
            function.callInit(properties == null ? Collections.emptyMap() : properties, ranges == null ? Collections.emptyMap() : ranges);
            return function;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    public ColumnJoinerRuleFunction getColumnJoinerRuleFunction(String name, String clazz, Map<String, String> properties, Map<String, String> ranges) {
        try {
            RuleFunction function = createFunction(name, clazz);
            function.callInit(properties == null ? Collections.emptyMap() : properties, ranges == null ? Collections.emptyMap() : ranges);
            return new ColumnJoinerRuleFunction(name,function);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
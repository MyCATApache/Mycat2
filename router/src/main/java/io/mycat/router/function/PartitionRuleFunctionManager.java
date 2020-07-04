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
import io.mycat.router.CustomRuleFunction;
import io.mycat.router.ShardingTableHandler;

import java.util.Collections;
import java.util.Map;

public enum PartitionRuleFunctionManager {
    INSTANCE;

    public static CustomRuleFunction createFunction(String name, String clazz)
            throws Exception {
        Class<?> clz = Class.forName(clazz);
        //判断是否继承AbstractPartitionAlgorithm
        if (!CustomRuleFunction.class.isAssignableFrom(clz)) {
            throw new IllegalArgumentException("rule function must implements "
                    + CustomRuleFunction.class.getName() + ", name=" + name);
        }
        return (CustomRuleFunction) clz.getDeclaredConstructor().newInstance();
    }

    public static CustomRuleFunction getRuleAlgorithm(ShardingTableHandler tableHandler,String columnName, SharingFuntionRootConfig.ShardingFuntion funtion)
            throws Exception {
        Map<String, String> properties = funtion.getProperties();
        properties = (properties == null) ? Collections.emptyMap() : properties;
        funtion.setProperties(properties);
        CustomRuleFunction rootFunction = createFunction(funtion.getName(), funtion.getClazz());
        rootFunction.callInit(tableHandler, columnName, funtion.getProperties(), funtion.getRanges());
        return rootFunction;
    }

    public CustomRuleFunction getRuleAlgorithm(ShardingTableHandler tableHandler,String defaultColumnName, String clazz, Map<String, String> properties, Map<String, String> ranges) {
        try {
            CustomRuleFunction function = createFunction(defaultColumnName, clazz);
            function.callInit(tableHandler,defaultColumnName,properties == null ? Collections.emptyMap() : properties, ranges == null ? Collections.emptyMap() : ranges);
            return function;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
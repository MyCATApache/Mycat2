/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.ImplementableFunction;
import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ReflectiveFunctionBase;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.util.ReflectUtil;

import java.lang.reflect.Method;
import java.util.List;

public class MycatScalarFunction implements ScalarFunction, ImplementableFunction {
    final ScalarFunctionImpl scalarFunction;

    public MycatScalarFunction(ScalarFunctionImpl scalarFunction) {
        this.scalarFunction = scalarFunction;
    }

    public static ScalarFunction create(Class<?> clazz, int paramNum) {
        return create(clazz, "eval", paramNum);
    }

    public static ScalarFunction create(Class<?> clazz, String methodName, int paramNum) {
        final Method method = findMethod(clazz, methodName);
        if (method == null) {
            return null;
        }
        return create(method, paramNum);
    }

    static Method findMethod(Class<?> clazz, String name) {
        for (Method method : clazz.getMethods()) {
            if (method.getName().equals(name) && !method.isBridge()) {
                return method;
            }
        }
        return null;
    }

    public static ScalarFunction create(Method method, int paramNum) {
        ScalarFunctionImpl scalarFunction = (ScalarFunctionImpl) ScalarFunctionImpl.create(method);
        int parameterCount = paramNum;
        if (method.getParameterCount() == 1 && method.getParameterTypes()[0].isArray()) {
            Class type = method.getParameterTypes()[0].getComponentType();
            ReflectiveFunctionBase.ParameterListBuilder builder = new ReflectiveFunctionBase.ParameterListBuilder();
            String parameterName = ReflectUtil.getParameterName(method, 0);
            for (int i = 0; i < parameterCount; i++) {
                builder.add(type, parameterName + i);
            }
            ImmutableList<FunctionParameter> parameters = builder.build();
            return new MycatScalarFunction(scalarFunction) {
                @Override
                public List<FunctionParameter> getParameters() {
                    return parameters;
                }
            };
        }
        return scalarFunction;
    }

    @Override
    public CallImplementor getImplementor() {
        return scalarFunction.getImplementor();
    }

    @Override
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
        return scalarFunction.getReturnType(typeFactory);
    }

    @Override
    public List<FunctionParameter> getParameters() {
        return scalarFunction.getParameters();
    }
}
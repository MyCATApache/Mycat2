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

import com.google.common.collect.ImmutableMap;
import io.mycat.calcite.executor.MycatScalar;
import org.apache.calcite.MycatContext;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.ParameterExpression;
import org.apache.calcite.linq4j.tree.Types;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

/**
 * Built-in methods.
 */
public enum MycatBuiltInMethod {
    SCALAR_EXECUTE1(MycatScalar.class, "execute", MycatContext.class),
    SCALAR_EXECUTE2(MycatScalar.class, "execute", MycatContext.class, Object[].class),
    CONTEXT_VALUES(MycatContext.class, "values", true)

//    CONTEXT_ROOT(MycatContext.class, "root", true);
;
    public final Method method;
    public final Constructor constructor;
    public final Field field;

    public static final ImmutableMap<Method, MycatBuiltInMethod> MAP;
   public static ParameterExpression ROOT =
            Expressions.parameter(Modifier.FINAL, MycatContext.class, "context");
    static {
        final ImmutableMap.Builder<Method, MycatBuiltInMethod> builder =
                ImmutableMap.builder();
        try {
            for (MycatBuiltInMethod value : MycatBuiltInMethod.values()) {
                if (value.method != null) {
                    builder.put(value.method, value);
                }
            }
        }catch (Throwable t){
            t.printStackTrace();
        }
        MAP = builder.build();
    }

    MycatBuiltInMethod(Method method, Constructor constructor, Field field) {
        this.method = method;
        this.constructor = constructor;
        this.field = field;
    }

    /**
     * Defines a method.
     */
    MycatBuiltInMethod(Class clazz, String methodName, Class... argumentTypes) {
        this(Types.lookupMethod(clazz, methodName, argumentTypes), null, null);
    }

    /**
     * Defines a constructor.
     */
    MycatBuiltInMethod(Class clazz, Class... argumentTypes) {
        this(null, Types.lookupConstructor(clazz, argumentTypes), null);
    }

    /**
     * Defines a field.
     */
    MycatBuiltInMethod(Class clazz, String fieldName, boolean dummy) {
        this(null, null, Types.lookupField(clazz, fieldName));
        assert dummy : "dummy value for method overloading must be true";
    }

    public String getMethodName() {
        return method.getName();
    }
}

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

import com.google.common.collect.ImmutableMultimap;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.logical.MycatViewSqlString;
import io.mycat.util.JsonUtil;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.runtime.Utilities;
import org.apache.calcite.sql.util.SqlString;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@Getter
public class CodeExecuterContext implements Serializable {

    RelContext relContext;
    Map<String, Object> varContext;
    CodeContext codeContext;
    private final transient ArrayBindable bindable;

    public CodeExecuterContext(RelContext relContext,
                               Map<String, Object> varContext,
                               CodeContext codeContext) {
        this.relContext = relContext;
        this.varContext = varContext;
        this.codeContext = codeContext;
        this.bindable = asObjectArray(getBindable(codeContext));
    }

    public static final CodeExecuterContext of(
            RelContext relContext,
            Map<String, Object> context,
            CodeContext codeContext
    ) {
        return new CodeExecuterContext(relContext, context, codeContext);
    }

    public MycatRowMetaData get(String name) {
        return relContext.nodes.get(name).columnInfo;
    }

    public ImmutableMultimap<String, SqlString> expand(String relNode, List<Object> params) {
        Objects.requireNonNull(relContext);
        Objects.requireNonNull(relContext.nodes);
        MycatViewSqlString apply = Objects.requireNonNull(relContext.nodes).get(relNode).dataNodeMapping.apply(params);
        return apply.getSqls();
    }

    public ArrayBindable getBindable() {
        return bindable;
    }

    @NotNull
    private static ArrayBindable asObjectArray(ArrayBindable bindable) {
        if (bindable.getElementType().isArray()) {
            return bindable;
        }
        return new ArrayBindable() {
            @Override
            public Class<Object[]> getElementType() {
                return Object[].class;
            }

            @Override
            public Enumerable<Object[]> bind(NewMycatDataContext dataContext) {
                Enumerable enumerable = bindable.bind(dataContext);
                return enumerable.select(e -> {
                    return new Object[]{e};
                });
            }
        };
    }

    @SneakyThrows
    static ArrayBindable getBindable(CodeContext codeContext) {
        ICompilerFactory compilerFactory;
        try {
            compilerFactory = CompilerFactoryFactory.getDefaultCompilerFactory();
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Unable to instantiate java compiler", e);
        }
        final IClassBodyEvaluator cbe = compilerFactory.newClassBodyEvaluator();
        cbe.setClassName(codeContext.getName());
        cbe.setExtendedClass(Utilities.class);
        cbe.setImplementedInterfaces(new Class[]{ArrayBindable.class});
        cbe.setParentClassLoader(EnumerableInterpretable.class.getClassLoader());
        if (CalciteSystemProperty.DEBUG.value()) {
            // Add line numbers to the generated janino class
            cbe.setDebuggingInformation(true, true, true);
        }
        return (ArrayBindable) cbe.createInstance(new StringReader(codeContext.getCode()));
    }

//    public String toJson() {
//        Map<String, String> map = new HashMap<>();
//        map.put("relContext", relContext.toJson());
//        map.put("varContext", JsonUtil.toJson(varContext));
//        map.put("codeContext", codeContext.toJson());
//        return JsonUtil.toJson(map);
//    }

    public CodeExecuterContext fromJson(String json) {
        Map<String,String> map = JsonUtil.from(json, Map.class);
        String relContext = map.get("relContext");
        String codeContext = map.get("codeContext");
//
//        Map<String,Object> varContext = JsonUtil.from( map.get("varContext"), Map.class);
//        map.put("relContext", this.relContext.toJson());
//        map.put("varContext", JsonUtil.toJson(varContext));
//        map.put("codeContext", codeContext.toJson());
        return null;
    }

    public MycatRel getMycatRel() {
        return relContext.relNode;
    }
}
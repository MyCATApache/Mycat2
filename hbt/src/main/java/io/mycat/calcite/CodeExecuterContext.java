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
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.util.JsonUtil;
import lombok.Getter;
import lombok.SneakyThrows;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.config.CalciteSystemProperty;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.runtime.NewMycatDataContext;
import org.apache.calcite.runtime.Utilities;
import org.codehaus.commons.compiler.CompilerFactoryFactory;
import org.codehaus.commons.compiler.IClassBodyEvaluator;
import org.codehaus.commons.compiler.ICompilerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.io.StringReader;
import java.util.Map;

@Getter
public class CodeExecuterContext implements Serializable {

    private Map<RexNode, RexNode> constantMap;
    Map<String, MycatRelDatasourceSourceInfo> relContext;
    Map<String, Object> varContext;
    MycatRel mycatRel;
    CodeContext codeContext;
    public transient PrepareExecutor bindable;

    public CodeExecuterContext(Map<RexNode, RexNode> constantMap,
                               Map<String, MycatRelDatasourceSourceInfo> relContext,
                               Map<String, Object> varContext,
                               MycatRel mycatRel,
                               CodeContext codeContext) {
        this.constantMap = constantMap;
        this.relContext = relContext;
        this.varContext = varContext;
        this.mycatRel = mycatRel;
        this.codeContext = codeContext;
    }

    public static final CodeExecuterContext of(
            Map<RexNode, RexNode> constantMap,
            Map<String, MycatRelDatasourceSourceInfo> relContext,
            Map<String, Object> context,
            MycatRel mycatRel,
            CodeContext codeContext
    ) {
        return new CodeExecuterContext(constantMap, relContext, context, mycatRel, codeContext);
    }

    public MycatRowMetaData get(String name) {
        return relContext.get(name).getColumnInfo();
    }


    public MycatRel getMycatRel() {
        return mycatRel;
    }
}
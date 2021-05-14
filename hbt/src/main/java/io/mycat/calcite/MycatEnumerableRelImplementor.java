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

import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.function.Function1;
import org.apache.calcite.linq4j.tree.*;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.BuiltInMethod;

import java.lang.reflect.Modifier;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;

public class MycatEnumerableRelImplementor extends EnumerableRelImplementor {


    public MycatEnumerableRelImplementor(Map<String, Object> internalParameters) {
        super(MycatCalciteSupport.RexBuilder, internalParameters);
    }

    public Function1<String, RexToLixTranslator.InputGetter> getAllCorrelateVariablesFunction() {
        return allCorrelateVariables;
    }

    @Override
    public ClassDeclaration implementRoot(EnumerableRel rootRel,
                                          EnumerableRel.Prefer prefer) {
        EnumerableRel.Result result;
        try {
            result = rootRel.implement(this, prefer);
        } catch (RuntimeException e) {
            IllegalStateException ex = new IllegalStateException("Unable to implement "
                    + RelOptUtil.toString(rootRel, SqlExplainLevel.ALL_ATTRIBUTES));
            ex.addSuppressed(e);
            throw ex;
        }
        final List<MemberDeclaration> memberDeclarations = new ArrayList<>();
        new TypeRegistrar(memberDeclarations).go(result);

        // This creates the following code
        // final Integer v1stashed = (Integer) root.get("v1stashed")
        // It is convenient for passing non-literal "compile-time" constants
        final Collection<Statement> stashed =
                Collections2.transform(stashedParameters.values(),
                        input -> Expressions.declare(Modifier.FINAL, input,
                                Expressions.convert_(
                                        Expressions.call(DataContext.ROOT,
                                                BuiltInMethod.DATA_CONTEXT_GET.method,
                                                Expressions.constant(input.name)),
                                        input.type)));

        final BlockStatement block = Expressions.block(
                Iterables.concat(
                        stashed,
                        result.block.statements));
        memberDeclarations.add(
                Expressions.methodDecl(
                        Modifier.PUBLIC,
                        Enumerable.class,
                        BuiltInMethod.BINDABLE_BIND.method.getName(),
                        Expressions.list(DataContext.ROOT),
                        block));
        memberDeclarations.add(
                Expressions.methodDecl(Modifier.PUBLIC, Class.class,
                        BuiltInMethod.TYPED_GET_ELEMENT_TYPE.method.getName(),
                        ImmutableList.of(),
                        Blocks.toFunctionBlock(
                                Expressions.return_(null,
                                        Expressions.constant(result.physType.getJavaRowType())))));
        return Expressions.classDecl(Modifier.PUBLIC,
                "Baz",
                null,
                Collections.singletonList(Bindable.class),
                memberDeclarations);
    }
}

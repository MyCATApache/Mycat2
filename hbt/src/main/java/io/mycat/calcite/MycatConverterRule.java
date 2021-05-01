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

import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.tools.RelBuilderFactory;

import java.util.function.Predicate;

/**
 * Abstract base class for rule that converts to Mycat.
 */
public abstract class MycatConverterRule extends ConverterRule {
    protected final MycatConvention out;

   public  <R extends RelNode> MycatConverterRule(Class<R> clazz,
                                           Predicate<? super R> predicate, RelTrait in, MycatConvention out,
                                           RelBuilderFactory relBuilderFactory, String description) {
        super(clazz, predicate, in, out, relBuilderFactory, description);
        this.out = out;
    }
}
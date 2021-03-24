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
import io.mycat.MycatDataContext;
import io.mycat.calcite.logical.MycatView;
import io.mycat.calcite.table.MycatTransientSQLTableScan;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.ArrayBindable;
import org.apache.calcite.sql.util.SqlString;

import java.util.*;

@Getter
public class CodeExecuterContext {
    private IdentityHashMap<RelNode,Integer> mycatViews;
    final Map<String, Object> context;
    final ArrayBindable bindable;
    final String code;
    final boolean forUpdate;


    public CodeExecuterContext(IdentityHashMap<RelNode,Integer> mycatViews, Map<String, Object> context, ArrayBindable bindable, String code, boolean forUpdate) {
        this.mycatViews = mycatViews;
        this.context = context;
        this.bindable = bindable;
        this.code = code;
        this.forUpdate = forUpdate;
    }

    public static final CodeExecuterContext of(IdentityHashMap<RelNode,Integer> mycatViews, Map<String, Object> context,
                                               ArrayBindable bindable,
                                               String code, boolean forUpdate) {
        return new CodeExecuterContext(mycatViews, context, bindable, code, forUpdate);
    }

}
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
package io.mycat.calcite.table;

import io.mycat.ProcedureHandler;
import io.mycat.TableHandler;
import io.mycat.ViewHandler;
import io.mycat.util.NameMap;

public class SchemaHandlerImpl implements SchemaHandler {
    final NameMap<TableHandler> tableMap = new NameMap<>();
    private String name;
    final String defaultTargetName;
    final NameMap<ProcedureHandler> procedureMap = new NameMap<>();
    final NameMap<ViewHandler> viewMap = new NameMap<>();

    public SchemaHandlerImpl(String name,String defaultTargetName) {
        this.name = name;
        this.defaultTargetName = defaultTargetName;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public NameMap< TableHandler> logicTables() {
        return tableMap;
    }

    @Override
    public String defaultTargetName() {
        return defaultTargetName;
    }

    @Override
    public NameMap<ProcedureHandler> procedures() {
        return procedureMap;
    }

    @Override
    public NameMap<ViewHandler> views() {
        return viewMap;
    }
}
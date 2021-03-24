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

import org.apache.calcite.plan.RelOptCluster;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class DualCustomTableHandler implements CustomTableHandler {
    private final LogicTable logicTable;
    private final Map map;
    private final List list;

    public DualCustomTableHandler(LogicTable logicTable,
                                  java.util.Map map,
                                  java.util.List list){

        this.logicTable = logicTable;
        this.map = map;
        this.list = list;
    }
    @Override
    public Supplier<Number> nextSequence() {
        return null;
    }

    @Override
    public void createPhysicalTables() {

    }

    @Override
    public void dropPhysicalTables() {

    }

    @Override
    public Long insert(Object[] row) {
        return null;
    }

    @Override
    public void replace(Object[] original, Object[] now) {

    }

    @Override
    public QueryBuilder createQueryBuilder(RelOptCluster cluster) {
        return QueryBuilder.createDefaultQueryBuilder(cluster,"mycat.dual",
                Collections.singletonList(new Object[]{}));
    }
}

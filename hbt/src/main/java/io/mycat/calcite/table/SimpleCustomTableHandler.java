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

import java.util.*;
import java.util.function.Supplier;

public class SimpleCustomTableHandler implements CustomTableHandler {

    private final LogicTable logicTable;
    private final Map<String, Object> kvOptions;
    private final List<Object> listOptions;

    private Collection<Object[]> collection = new ArrayList<>();

    public SimpleCustomTableHandler(LogicTable logicTable,
                                    Map<String, Object> kvOptions,
                                    List<Object> listOptions) {
        this.logicTable = logicTable;
        this.kvOptions = kvOptions;
        this.listOptions = listOptions;
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
        collection.add(row);
        return null;
    }

    @Override
    public void replace(Object[] original, Object[] now) {
        for (Object[] e : collection) {
            if (Arrays.deepEquals(original, e)) {
                System.arraycopy(now, 0, e, 0, e.length);
                break;
            }
        }
    }

    @Override
    public QueryBuilder createQueryBuilder(RelOptCluster cluster) {
        return QueryBuilder.createDefaultQueryBuilder(cluster,
                logicTable.getUniqueName(),
                collection);
    }
}

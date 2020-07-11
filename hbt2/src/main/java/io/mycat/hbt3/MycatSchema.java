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
package io.mycat.hbt3;

import com.google.common.collect.ImmutableMap;
import io.mycat.TableHandler;
import lombok.SneakyThrows;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;


public class MycatSchema extends AbstractSchema {
    private final ImmutableMap<String, Table> tables;
    private final ImmutableMap<String, MycatTable> mycatTables;
    private final Collection<TableHandler> collect;

    public MycatSchema(Collection<TableHandler> collect) {
        this.collect = collect.stream().distinct().collect(Collectors.toList());
        final ImmutableMap.Builder<String, Table> tablebuilder = ImmutableMap.builder();
        final ImmutableMap.Builder<String, MycatTable> mycatTableBuilder = ImmutableMap.builder();
        for (TableHandler collectionName : this.collect) {
            String tableName = collectionName.getTableName();
            MycatTable mycatTable = new MycatTable(collectionName);
            mycatTableBuilder.put(tableName, mycatTable);
            tablebuilder.put(tableName, mycatTable);
        }
        this.tables = tablebuilder.build();
        this.mycatTables = mycatTableBuilder.build();
    }

    @SneakyThrows
    @Override
    protected Map<String, Table> getTableMap() {
        return tables;
    }

  public Map<String, MycatTable> getMycatTables() {
    return mycatTables;
  }
}

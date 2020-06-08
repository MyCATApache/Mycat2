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
package io.mycat.optimizer;

import com.google.common.collect.ImmutableMap;
import io.mycat.RootHelper;
import io.mycat.metadata.MetadataManager;
import io.mycat.metadata.TableHandler;
import lombok.SneakyThrows;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class MycatSchema extends AbstractSchema {
  private final ImmutableMap<String, Table> tables;
  private final Collection<TableHandler> collect;
  public MycatSchema(Collection<TableHandler> collect) {
    this.collect = collect.stream().distinct().collect(Collectors.toList());
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    for (TableHandler collectionName :  this.collect) {
      builder.put(collectionName.getTableName(), new MycatTable(collectionName));
    }
    this .tables = builder.build();
  }


  @SneakyThrows
  @Override protected Map<String, Table> getTableMap() {
    return tables;
  }
}

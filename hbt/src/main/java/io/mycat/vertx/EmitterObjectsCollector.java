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
package io.mycat.vertx;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.reactivex.rxjava3.core.ObservableEmitter;
import io.vertx.mysqlclient.impl.MySQLRowDesc;
import io.vertx.mysqlclient.impl.codec.StreamMysqlCollector;
import io.vertx.sqlclient.Row;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class EmitterObjectsCollector implements StreamMysqlCollector {
    protected final ObservableEmitter<Object[]> emitter;
    protected MycatRowMetaData rowMetaData;
    protected int currentRowCount;

    public EmitterObjectsCollector(ObservableEmitter<Object[]> emitter,
                                   MycatRowMetaData rowMetaData) {
        this.emitter = emitter;
        this.rowMetaData = rowMetaData;
    }

    @Override
    public void onColumnDefinitions(MySQLRowDesc columnDefinitions) {
        if (this.rowMetaData == null) {
            this.rowMetaData = BaseRowObservable.toColumnMetaData(
                    columnDefinitions.columnDescriptor());
        }
    }

    @Override
    public void onRow(Row row) {
        currentRowCount++;
        emitter.onNext(BaseRowObservable.getObjects(row, rowMetaData));
    }

}

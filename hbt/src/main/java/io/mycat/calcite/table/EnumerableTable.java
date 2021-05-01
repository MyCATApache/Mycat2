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

import io.mycat.Identical;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.calcite.MycatCalciteSupport;
import io.mycat.calcite.resultset.CalciteRowMetaData;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

public abstract class EnumerableTable extends AbstractTable implements ScannableTable, Identical {
    volatile Enumerable<Object[]> enumerable;
    public void setEnumerable(Enumerable<Object[]> enumerable) {
        this.enumerable = enumerable;
    }

    public MycatRowMetaData getMetaData() {
        return new CalciteRowMetaData(getRowType(MycatCalciteSupport.INSTANCE.TypeFactory).getFieldList());
    }
    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return enumerable;
    }

    public boolean existsEnumerable(){
        return enumerable!=null;
    }

}
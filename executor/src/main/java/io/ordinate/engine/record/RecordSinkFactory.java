/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.record;

import io.ordinate.engine.builder.SortOptions;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.record.impl.RecordSinkFactoryImpl;
import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.schema.IntInnerType;

import java.util.Comparator;

public interface RecordSinkFactory {

    public RecordSink buildRecordSink(IntInnerType[] types);
    public FunctionSink buildFunctionSink(Function[] functions);
    public RecordSetter getRecordSinkSPI(Object mapKey);
    public RecordComparator buildEqualComparator(InnerType[] types);
    public static final RecordSinkFactory INSTANCE = new RecordSinkFactoryImpl();
}

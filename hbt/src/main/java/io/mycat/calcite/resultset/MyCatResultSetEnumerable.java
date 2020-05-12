/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite.resultset;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.calcite.MycatCalciteDataContext;
import io.mycat.future.Future;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
public class MyCatResultSetEnumerable<T> extends AbstractEnumerable<T> {
    private final static Logger LOGGER = LoggerFactory.getLogger(MyCatResultSetEnumerable.class);
    private final MycatCalciteDataContext context;
    private final Future<RowBaseIterator> rowBaseIteratorFuture;
    private final AtomicBoolean CANCEL_FLAG;


    public MyCatResultSetEnumerable(MycatCalciteDataContext context, Future<RowBaseIterator> rowBaseIteratorFuture) {
        this.context = context;
        this.rowBaseIteratorFuture = rowBaseIteratorFuture;
        this. CANCEL_FLAG = context.getCancelFlag();
    }

    @Override
    public Enumerator<T> enumerator() {
        if (!rowBaseIteratorFuture.isComplete()) {
            throw new AssertionError("rowBaseIteratorFuture is not completed");
        }

        final RowBaseIterator rowBaseIterator = rowBaseIteratorFuture.result();
        final int columnCount = rowBaseIterator.getMetaData().getColumnCount();
        Enumerator<T> enumerator = new Enumerator<T>() {
            @Override
            public T current() {
                Object[] res = new Object[columnCount];
                for (int i = 0, j = 1; i < columnCount; i++, j++) {
                    Object object = rowBaseIterator.getObject(j);
                    if (object instanceof Date) {
                        res[i] = ((Date) object).getTime();
                    } else {
                        res[i] = object;
                    }
                }
                return (T) res;
            }

            @Override
            public boolean moveNext() {
                if (CANCEL_FLAG.get()) {
                    return false;
                }
                boolean result = rowBaseIterator.next();
                if (result) {
                    return result;
                }
                return result;
            }

            @Override
            public void reset() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                if (rowBaseIterator != null) {
                    rowBaseIterator.close();
                }
            }
        };
        return enumerator;
    }
}
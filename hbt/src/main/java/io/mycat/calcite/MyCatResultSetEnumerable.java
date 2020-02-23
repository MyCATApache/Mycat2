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
package io.mycat.calcite;

import io.mycat.QueryBackendTask;
import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
public class MyCatResultSetEnumerable<T> extends AbstractEnumerable<T> {
    private final MycatCalciteDataContext dataContext;
    private final AtomicBoolean CANCEL_FLAG;
    private final List<QueryBackendTask> backStoreList;
    private final static Logger LOGGER = LoggerFactory.getLogger(MyCatResultSetEnumerable.class);

    public MyCatResultSetEnumerable(MycatCalciteDataContext dataContext, List<QueryBackendTask> res) {
        this.dataContext = dataContext;
        this.backStoreList = res;
        this.CANCEL_FLAG = DataContext.Variable.CANCEL_FLAG.get(dataContext);
        for (QueryBackendTask sql : res) {
            LOGGER.info("prepare querySQL:{}", sql);
        }
    }

    public MyCatResultSetEnumerable(MycatCalciteDataContext dataContext, QueryBackendTask res) {
        this(dataContext, Collections.singletonList(res));
    }

    @Override
    public Enumerator<T> enumerator() {
        int length = backStoreList.size();

        ArrayList<RowBaseIterator> iterators = new ArrayList<>(length);
        for (QueryBackendTask endTableInfo : backStoreList) {
            DefaultConnection session = dataContext.getTarget(endTableInfo);
            iterators.add(session.executeQuery(endTableInfo.getSql()));
            LOGGER.info("runing querySQL:{}", endTableInfo.getSql());
        }

        return new Enumerator<T>() {
            RowBaseIterator currentrs;

            public T current() {
                final int columnCount = currentrs.metaData().getColumnCount();
                Object[] res = new Object[columnCount];
                for (int i = 0, j = 1; i < columnCount; i++, j++) {
                    Object object = currentrs.getObject(j);
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
                boolean result = false;
                while (!iterators.isEmpty()) {
                    currentrs = iterators.get(0);
                    result = currentrs.next();
                    if (result) {
                        return result;
                    }
                    iterators.remove(0);
                }
                return result;
            }

            @Override
            public void reset() {
                throw new UnsupportedOperationException();
            }

            @Override
            public void close() {
                currentrs.close();
            }
        };
    }
}
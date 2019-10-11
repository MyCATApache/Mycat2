/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.calcite;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.datasource.DsConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Date;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
/**
 * @author Weiqing Xu
 * @author Junwen Chen
 **/
public class MyCatResultSetEnumerable<T> extends AbstractEnumerable<T> {
    private final AtomicBoolean cancelFlag;
    private final List<BackendTableInfo> backStoreList;
    private final String[] sqls;
    private final static Logger LOGGER = LoggerFactory.getLogger(MyCatResultSetEnumerable.class);

    public MyCatResultSetEnumerable(AtomicBoolean cancelFlag, List<BackendTableInfo> backStoreList, String text, String filterText) {
        this.cancelFlag = cancelFlag;
        this.backStoreList = backStoreList;
        this.sqls = new String[backStoreList.size()];

        for (int i = 0; i < this.sqls.length; i++) {
            BackendTableInfo endTableInfo = backStoreList.get(i);
            String schemaName = endTableInfo.getSchemaInfo().getTargetSchema();
            String tableName = endTableInfo.getSchemaInfo().getTargetTable();
            String sql;
            if (filterText != null && !"".equals(filterText)) {
                sql = "select " + text +
                        " from " + schemaName + "." + tableName + " where " + filterText;
            } else {
                sql = "select " + text +
                        " from " + schemaName + "." + tableName;
            }
            this.sqls[i] = sql;
        }
        for (String sql : sqls) {
            LOGGER.info("run query:" + sql);
        }


    }

    @Override
    public Enumerator<T> enumerator() {
        int length = sqls.length;
        ArrayList<DsConnection> dsConnections = new ArrayList<>(length);
        ArrayList<RowBaseIterator> iterators = new ArrayList<>(length);
        for (int i = 0; i < length; i++) {
            BackendTableInfo endTableInfo = backStoreList.get(i);
            DsConnection session = endTableInfo.getSession(false,null);
            dsConnections.add(session);
            iterators.add(session.executeQuery(sqls[i]));
        }

        return new Enumerator<T>() {
            RowBaseIterator currentrs;


            public T current() {
                final int columnCount = currentrs.metaData().getColumnCount();
                Object[] res = new Object[columnCount];
                for (int i = 0, j = 1; i < columnCount; i++, j++) {
                    Object object = currentrs.getObject(j);
                    if (object instanceof Date){
                        res[i] = ((Date) object).getTime();
                    }else {
                        res[i] = object;
                    }
                }
                return (T) res;
            }

            @Override
            public boolean moveNext() {
                if (cancelFlag.get()) {
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
                for (RowBaseIterator iterator : iterators) {
                    iterator.close();
                }
                iterators.clear();
                for (DsConnection dsConnection : dsConnections) {
                    dsConnection.close();
                }
                dsConnections.clear();
            }
        };
    }
}
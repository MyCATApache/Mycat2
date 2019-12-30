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
import io.mycat.calcite.shardingQuery.BackendTask;
import io.mycat.datasource.jdbc.datasource.DefaultConnection;
import io.mycat.datasource.jdbc.thread.GThread;
import io.mycat.replica.PhysicsInstanceImpl;
import io.mycat.replica.ReplicaSelectorRuntime;
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
    private final  List<BackendTask>  backStoreList;
    private final static Logger LOGGER = LoggerFactory.getLogger(MyCatResultSetEnumerable.class);

    public MyCatResultSetEnumerable(AtomicBoolean cancelFlag, List<BackendTask> res) {
        this.cancelFlag = cancelFlag;
        this.backStoreList = res;
        for (BackendTask sql : res) {
            LOGGER.info("prepare query:{}", sql);
        }
    }

    @Override
    public Enumerator<T> enumerator() {
        int length = backStoreList.size();
        ArrayList<DefaultConnection> dsConnections = new ArrayList<>(length);
        ArrayList<RowBaseIterator> iterators = new ArrayList<>(length);
        for (BackendTask endTableInfo : backStoreList) {
            GThread gThread = (GThread) Thread.currentThread();
            PhysicsInstanceImpl datasourceByReplicaName = ReplicaSelectorRuntime.INSTANCE.getDatasourceByReplicaName(endTableInfo.getBackendTableInfo().getReplicaName());
            DefaultConnection session = gThread.getTransactionSession().getConnection(datasourceByReplicaName.getName());
            dsConnections.add(session);
            iterators.add(session.executeQuery(endTableInfo.getSql()));
            LOGGER.info("runing query:{}", endTableInfo.getSql());
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
                for (DefaultConnection dsConnection : dsConnections) {
                    dsConnection.close();
                }
                dsConnections.clear();
            }
        };
    }
}
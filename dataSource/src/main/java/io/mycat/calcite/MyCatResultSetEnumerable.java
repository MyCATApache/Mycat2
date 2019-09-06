package io.mycat.calcite;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.datasource.jdbc.GRuntime;
import io.mycat.datasource.jdbc.datasource.DsConnection;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class MyCatResultSetEnumerable<T> extends AbstractEnumerable<T> {
    private final AtomicBoolean cancelFlag;
    private final List<BackEndTableInfo> backStoreList;
    private final String[] sqls;
    private final static Logger LOGGER = LoggerFactory.getLogger(MyCatResultSetEnumerable.class);

    public MyCatResultSetEnumerable(AtomicBoolean cancelFlag, List<BackEndTableInfo> backStoreList, String text, String filterText) {
        this.cancelFlag = cancelFlag;
        this.backStoreList = backStoreList;
        this.sqls = new String[backStoreList.size()];

        for (int i = 0; i < this.sqls.length; i++) {
            BackEndTableInfo endTableInfo = backStoreList.get(i);
            String schemaName = endTableInfo.getSchemaName();
            String tableName = endTableInfo.getTableName();
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
            BackEndTableInfo endTableInfo = backStoreList.get(i);
            DsConnection session = GRuntime.INSTACNE.getJdbcDatasourceSessionByName(endTableInfo.getHostName());
            dsConnections.add(session);
            iterators.add(session.executeQuery(sqls[i]));
        }

        return new Enumerator<T>() {
            RowBaseIterator currentrs;


            public T current() {
                final int columnCount = currentrs.metaData().getColumnCount();
                Object[] res = new Object[columnCount];
                for (int i = 0, j = 1; i < columnCount; i++, j++) {
                    res[i] = currentrs.getObject(j);
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
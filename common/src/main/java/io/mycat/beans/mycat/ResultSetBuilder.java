package io.mycat.beans.mycat;

import io.mycat.api.collector.AbstractObjectRowIterator;
import io.mycat.api.collector.RowBaseIterator;

import java.io.Serializable;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ResultSetBuilder {
    final List<ColumnInfo> columnInfos = new ArrayList<>();
    final List<Object[]> objectList = new ArrayList<>();

    public ResultSetBuilder() {
        columnInfos.add(null);
    }

    public static ResultSetBuilder create() {
        return new ResultSetBuilder();
    }

    public ResultSetBuilder addColumnInfo(String schemaName, String tableName, String columnName, int columnType, int precision, int scale, String columnLabel, boolean isAutoIncrement, boolean isCaseSensitive, boolean isNullable, boolean isSigned, int displaySize) {
        columnInfos.add(new ColumnInfo(schemaName, tableName, columnName, columnType, precision, scale, columnLabel, isAutoIncrement, isCaseSensitive, isNullable, isSigned, displaySize));
        return this;
    }

    public ResultSetBuilder addColumnInfo(String tableName, String columnName, int columnType, int precision, int scale) {
        columnInfos.add(new ColumnInfo(tableName, tableName, columnName, columnType, precision, scale, columnName, false, true, true, true, columnName.length()));
        return this;
    }

    public ResultSetBuilder addColumnInfo(String columnName, JDBCType columnType) {
        addColumnInfo(columnName, columnType.getVendorTypeNumber());
        return this;
    }

    public ResultSetBuilder addColumnInfo(String columnName, int columnType) {
        columnInfos.add(new ColumnInfo(columnName, columnType));
        return this;
    }


    public int columnCount() {
        return columnInfos.size();
    }


    public void addObjectRowPayload(Object row) {
        objectList.add(new Object[]{row});
    }

    public void addObjectRowPayload(List row) {
        objectList.add(row.toArray());
    }

    public RowBaseIterator build() {
        SimpleDefMycatRowMetaData mycatRowMetaData = new SimpleDefMycatRowMetaData(columnInfos);
        int columnCount = mycatRowMetaData.getColumnCount();
        return new DefObjectRowIteratorImpl(mycatRowMetaData, objectList.iterator());
    }

    public RowBaseIterator build(MycatRowMetaData mycatRowMetaData) {
        return new DefObjectRowIteratorImpl(mycatRowMetaData, objectList.iterator());
    }

    /**
     * 跳过头部的null
     *
     * @return
     */
    public List<ColumnInfo> getColumnInfos() {
        return columnInfos.subList(1, columnInfos.size());
    }

    /**
     * @author Junwen Chen
     **/
    public static class SimpleDefMycatRowMetaData implements MycatRowMetaData, Serializable {
        final List<ColumnInfo> columnInfos;

        public SimpleDefMycatRowMetaData(List<ColumnInfo> columnInfos) {
            this.columnInfos = columnInfos;
        }

        @Override
        public int getColumnCount() {
            return columnInfos.size() - 1;
        }

        @Override
        public boolean isAutoIncrement(int column) {
            return columnInfos.get(column).isAutoIncrement();
        }

        @Override
        public boolean isCaseSensitive(int column) {
            return columnInfos.get(column).isCaseSensitive();
        }

        @Override
        public boolean isNullable(int column) {
            return columnInfos.get(column).isNullable();
        }

        @Override
        public boolean isSigned(int column) {
            return columnInfos.get(column).isSigned();
        }

        @Override
        public int getColumnDisplaySize(int column) {
            return columnInfos.get(column).getDisplaySize();
        }

        @Override
        public String getColumnName(int column) {
            return columnInfos.get(column).getColumnName();
        }

        @Override
        public String getSchemaName(int column) {
            return columnInfos.get(column).getSchemaName();
        }

        @Override
        public int getPrecision(int column) {
            return columnInfos.get(column).getPrecision();
        }

        @Override
        public int getScale(int column) {
            return columnInfos.get(column).getScale();
        }

        @Override
        public String getTableName(int column) {
            return columnInfos.get(column).getTableName();
        }

        @Override
        public int getColumnType(int column) {
            return columnInfos.get(column).getColumnType();
        }

        @Override
        public String getColumnLabel(int column) {
            return columnInfos.get(column).getColumnLabel();
        }

        @Override
        public ResultSetMetaData metaData() {
            throw new UnsupportedOperationException();
        }
    }

    static public class DefObjectRowIteratorImpl extends AbstractObjectRowIterator implements Serializable {
        final MycatRowMetaData mycatRowMetaData;
        final Iterator<Object[]> iterator;
        boolean close = false;

        public DefObjectRowIteratorImpl(MycatRowMetaData mycatRowMetaData, Iterator<Object[]> iterator) {
            this.mycatRowMetaData = mycatRowMetaData;
            this.iterator = iterator;
        }

        @Override
        public MycatRowMetaData getMetaData() {
            return mycatRowMetaData;
        }

        @Override
        public boolean next() {
            if (this.iterator.hasNext()) {
                this.currentRow = this.iterator.next();
                return true;
            } else {
                return false;
            }
        }

        @Override
        public void close() {
            close = true;
        }

    }
}
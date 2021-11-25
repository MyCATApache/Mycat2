package io.mycat.beans.mycat;

import java.sql.ResultSetMetaData;
import java.util.List;

public class RenameMycatRowMetaData implements MycatRowMetaData {

    final MycatRowMetaData mycatRowMetaData;
    final List<String> aliasList;

    public static RenameMycatRowMetaData of(MycatRowMetaData mycatRowMetaData, List<String> aliasList){
        return new RenameMycatRowMetaData(mycatRowMetaData,aliasList);
    }

    public RenameMycatRowMetaData(MycatRowMetaData mycatRowMetaData, List<String> aliasList) {
        this.mycatRowMetaData = mycatRowMetaData;
        this.aliasList = aliasList;
    }

    @Override
    public int getColumnCount() {
        return mycatRowMetaData.getColumnCount();
    }

    @Override
    public boolean isAutoIncrement(int column) {
        return mycatRowMetaData.isAutoIncrement(column);
    }

    @Override
    public boolean isCaseSensitive(int column) {
        return mycatRowMetaData.isCaseSensitive(column);
    }

    @Override
    public boolean isSigned(int column) {
        return mycatRowMetaData.isSigned(column);
    }

    @Override
    public int getColumnDisplaySize(int column) {
        return mycatRowMetaData.getColumnDisplaySize(column);
    }

    @Override
    public String getColumnName(int column) {
        return aliasList.get(column);
    }

    @Override
    public String getSchemaName(int column) {
        return mycatRowMetaData.getSchemaName(column);
    }

    @Override
    public int getPrecision(int column) {
        return mycatRowMetaData.getPrecision(column);
    }

    @Override
    public int getScale(int column) {
        return mycatRowMetaData.getScale(column);
    }

    @Override
    public String getTableName(int column) {
        return mycatRowMetaData.getTableName(column);
    }

    @Override
    public int getColumnType(int column) {
        return mycatRowMetaData.getColumnType(column);
    }

    @Override
    public String getColumnLabel(int column) {
        return aliasList.get(column);
    }

    @Override
    public ResultSetMetaData metaData() {
        return mycatRowMetaData.metaData();
    }

    @Override
    public boolean isNullable(int column) {
        return mycatRowMetaData.isNullable(column);
    }
}

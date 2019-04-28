package io.mycat.beans;

import io.mycat.beans.mysql.RowStoreType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MySQLTableInfo {
    String schemaName;
    String tableName;
    MySQLIndexes indexes;
    MySQLForeignKey foreignKey;
   final Map<String,MySQLFieldInfo> fieldInfoMap = new HashMap<>();
    List<Object[]> list = new ArrayList<>();

    public MySQLTableInfo() {
    }


    public void putField(MySQLFieldInfo fieldInfo){
        fieldInfoMap.put(fieldInfo.getName(),fieldInfo);
    }
    public void remove(String columnName){
        fieldInfoMap.remove(columnName);
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Map<String, MySQLFieldInfo> getFieldInfoMap() {
        return fieldInfoMap;
    }

    public List<Object[]> getList() {
        return list;
    }

    public void setList(List<Object[]> list) {
        this.list = list;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public void setIncrementStep(int incrementStep) {
        this.incrementStep = incrementStep;
    }

    public void setAverageRowLength(int averageRowLength) {
        this.averageRowLength = averageRowLength;
    }

    public void setMaxRowCount(long maxRowCount) {
        this.maxRowCount = maxRowCount;
    }

    public void setMinRowCount(long minRowCount) {
        this.minRowCount = minRowCount;
    }

    public void setRowStoreType(RowStoreType rowStoreType) {
        this.rowStoreType = rowStoreType;
    }

    public void setVerification(boolean verification) {
        this.verification = verification;
    }

    public void setDelayKeyWriting(boolean delayKeyWriting) {
        this.delayKeyWriting = delayKeyWriting;
    }

    String comment;
    int incrementStep;
    int averageRowLength;
    long maxRowCount;
    long minRowCount;
    RowStoreType rowStoreType;
    boolean verification;
    boolean delayKeyWriting;

    public String getSchemaName() {
        return schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public String getComment() {
        return comment;
    }

    public int getIncrementStep() {
        return incrementStep;
    }

    public int getAverageRowLength() {
        return averageRowLength;
    }

    public long getMaxRowCount() {
        return maxRowCount;
    }

    public long getMinRowCount() {
        return minRowCount;
    }

    public RowStoreType getRowStoreType() {
        return rowStoreType;
    }

    public boolean isVerification() {
        return verification;
    }

    public boolean isDelayKeyWriting() {
        return delayKeyWriting;
    }

    public MySQLIndexes getIndexes() {
        return indexes;
    }

    public void setIndexes(MySQLIndexes indexes) {
        this.indexes = indexes;
    }

    public MySQLForeignKey getForeignKey() {
        return foreignKey;
    }

    public void setForeignKey(MySQLForeignKey foreignKey) {
        this.foreignKey = foreignKey;
    }

}

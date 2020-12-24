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
package io.mycat.beans.mysql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jamie12221
 *  date 2019-05-05 16:22
 *
 * mysql 表信息
 **/
public class MySQLTableInfo {
    final Map<String, MySQLFieldInfo> fieldInfoMap = new HashMap<>();
    String schemaName;
    String tableName;
    MySQLIndexes indexes;
    MySQLForeignKey foreignKey;
    List<Object[]> list = new ArrayList<>();
    String comment;
    int incrementStep;
    int averageRowLength;
    long maxRowCount;
    long minRowCount;
    MySQLRowStoreType rowStoreType;
    boolean verification;
    boolean delayKeyWriting;
    MySQLTableType tableType;

    public MySQLTableInfo() {
    }

    public MySQLTableType getTableType() {
        return tableType;
    }

    public void setTableType(MySQLTableType tableType) {
        this.tableType = tableType;
    }

    public void putField(MySQLFieldInfo fieldInfo) {
        fieldInfoMap.put(fieldInfo.getName(), fieldInfo);
    }

    public void remove(String columnName) {
        fieldInfoMap.remove(columnName);
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

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getComment() {
        return comment;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    public int getIncrementStep() {
        return incrementStep;
    }

    public void setIncrementStep(int incrementStep) {
        this.incrementStep = incrementStep;
    }

    public int getAverageRowLength() {
        return averageRowLength;
    }

    public void setAverageRowLength(int averageRowLength) {
        this.averageRowLength = averageRowLength;
    }

    public long getMaxRowCount() {
        return maxRowCount;
    }

    public void setMaxRowCount(long maxRowCount) {
        this.maxRowCount = maxRowCount;
    }

    public long getMinRowCount() {
        return minRowCount;
    }

    public void setMinRowCount(long minRowCount) {
        this.minRowCount = minRowCount;
    }

    public MySQLRowStoreType getRowStoreType() {
        return rowStoreType;
    }

    public void setRowStoreType(MySQLRowStoreType rowStoreType) {
        this.rowStoreType = rowStoreType;
    }

    public boolean isVerification() {
        return verification;
    }

    public void setVerification(boolean verification) {
        this.verification = verification;
    }

    public boolean isDelayKeyWriting() {
        return delayKeyWriting;
    }

    public void setDelayKeyWriting(boolean delayKeyWriting) {
        this.delayKeyWriting = delayKeyWriting;
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

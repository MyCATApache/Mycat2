/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mysql;

/**
 * @author jamie12221
 *  date 2019-05-05 16:22
 *
 * 预处理语句 参数与值得绑定类
 **/
public class MySQLPStmtBindValueList {
    Object[] valueList;
    int[] parameterTypeList;
    private final MySQLPreparedStatement preparedStatement;
    private byte[] nullBitMap;

    public MySQLPStmtBindValueList(MySQLPreparedStatement preparedStatement) {
        this.preparedStatement = preparedStatement;
        this.parameterTypeList = preparedStatement.getParameterTypeList();
        this.valueList = new Object[parameterTypeList.length];
        this. nullBitMap  = new byte[(parameterTypeList.length + 7) / 8];
    }

    public Object[] getValueList() {
        return valueList;
    }

    public int[] getParameterTypeList() {
        return parameterTypeList;
    }

    public byte[] getCacheNullBitMap() {
        return nullBitMap;
    }

    public void put(int index, Object value) {
        int orgType = this.parameterTypeList[index];
        int actuallyType;
        Class<?> aClass = value.getClass();
        if (aClass == Long.class) {
            actuallyType = MySQLFieldsType.FIELD_TYPE_LONG;
        } else if (aClass == Byte.class) {
            actuallyType = MySQLFieldsType.FIELD_TYPE_TINY;
        } else if (aClass == Short.class) {
            actuallyType = MySQLFieldsType.FIELD_TYPE_SHORT;
        } else if (aClass == String.class) {
            actuallyType = MySQLFieldsType.FIELD_TYPE_STRING;
        } else if (aClass == Double.class) {
            actuallyType = MySQLFieldsType.FIELD_TYPE_DOUBLE;
        } else if (aClass == Float.class) {
            actuallyType = MySQLFieldsType.FIELD_TYPE_FLOAT;
        } else if (aClass == byte[].class) {
            preparedStatement.putLongDataForBuildLongData(index,(byte[]) value);
            actuallyType = MySQLFieldsType.FIELD_TYPE_BLOB;
        } else {
            throw new IllegalArgumentException("unsupport!");
        }
        if (actuallyType != orgType) {
            preparedStatement.setNewParameterBoundFlag(true);
            this.parameterTypeList[index] = actuallyType;
        }
        if (aClass != byte[].class){
            valueList[index] = value;
        }
    }

    public void reset() {
        preparedStatement.setNewParameterBoundFlag(false);
        for (int i = 0; i < valueList.length; i++) {
            valueList[i] = null;
        }
        for (int i = 0; i < nullBitMap.length; i++) {
            nullBitMap[i] = 0;
        }
    }
}

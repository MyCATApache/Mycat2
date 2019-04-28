package io.mycat.beans.mysql;

import io.mycat.proxy.MycatExpection;
import io.mycat.proxy.task.prepareStatement.PreparedStatement;

public class MySQLPStmtBindValueList {
    Object[] valueList;
    int[] parameterTypeList;
    private final PreparedStatement preparedStatement;
    private byte[] nullBitMap;

    public MySQLPStmtBindValueList(PreparedStatement preparedStatement) {
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
            throw new MycatExpection("unsupport!");
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

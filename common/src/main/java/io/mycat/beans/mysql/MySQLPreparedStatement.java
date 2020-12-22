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

import java.io.IOException;
import java.util.Map;

/**
 * CrazyPig,chenjunwen
 * 预处理语句信息
 */
public interface MySQLPreparedStatement {

    long getStatementId();

    int getColumnsNumber();

    int getParametersNumber();

    Map<Integer, MySQLPayloadWriter> getLongDataMap();

    boolean setNewParameterBoundFlag(boolean b);

    default MySQLPayloadWriter getLongData(int paramId) {
        return getLongDataMap().get(paramId);
    }

    int[] getParameterTypeList();

//    ColumnDefPacket[] getColumnDefList();

    /**
     * COM_STMT_RESET命令将调用该方法进行数据重置
     */
    default void resetLongData() {
        Map<Integer, MySQLPayloadWriter> longDataMap = getLongDataMap();
        int length = longDataMap.size();
        for (int i = 0; i < length; i++) {
            MySQLPayloadWriter byteArrayOutputStream = longDataMap.get(i);
            if (byteArrayOutputStream != null) {
                longDataMap.put(i, null);
            }
        }
        getBindValueList().reset();
    }

    default void putBlob(int index, byte[] value) {
        getBindValueList().put(index, value);
    }

    default void put(int index, Object value) {
        getBindValueList().put(index, value);
    }

    MySQLPStmtBindValueList getBindValueList();

    boolean isNewParameterBoundFlag();

    /**
     * 追加数据到指定的预处理参数
     */
    default void appendLongData(Integer paramId, byte[] data) throws IOException {
        Map<Integer, MySQLPayloadWriter> longDataMap = getLongDataMap();
        if (getLongData(paramId) == null) {
            MySQLPayloadWriter out = new MySQLPayloadWriter();
            out.write(data);
            longDataMap.put(paramId, out);
        } else {
            longDataMap.get(paramId).write(data);
        }
    }

    default void putLongDataForBuildLongData(int paramId, byte[] data) {
        Map<Integer, MySQLPayloadWriter> longDataMap = getLongDataMap();
        long statementId = this.getStatementId();

        MySQLPayloadWriter out = new MySQLPayloadWriter();
        out.write(0x18);
        out.writeFixInt(4, statementId);
        out.writeFixInt(2, paramId);
        out.write(data);

        longDataMap.put(paramId, out);
    }
}

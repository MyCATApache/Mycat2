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

import io.mycat.util.MySQLUtil;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Map;

/**
 * CrazyPig,chenjunwen
 */
public interface MySQLPreparedStatement {
    long getStatementId();

    int getColumnsNumber();

    int getParametersNumber();

    Map<Integer, ByteArrayOutputStream> getLongDataMap();

    boolean setNewParameterBoundFlag(boolean b);

    default public OutputStream getLongData(int paramId) {
        return getLongDataMap().get(paramId);
    }

    int[] getParameterTypeList();

//    ColumnDefPacket[] getColumnDefList();

    /**
     * COM_STMT_RESET命令将调用该方法进行数据重置
     */
    default public void resetLongData() {
        Map<Integer, ByteArrayOutputStream> longDataMap = getLongDataMap();
        int length = longDataMap.size();
        for (int i = 0; i < length; i++) {
            ByteArrayOutputStream byteArrayOutputStream = longDataMap.get(i);
            if (byteArrayOutputStream != null) {
                try {
                    byteArrayOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                longDataMap.put(i, null);
            }
        }
        getBindValueList().reset();
    }
    public default void putBlob(int index, byte[] value) {
        getBindValueList().put(index, value);
    }
    public default void put(int index, Object value) {
        getBindValueList().put(index, value);
    }
    MySQLPStmtBindValueList getBindValueList();

    boolean isNewParameterBoundFlag();

    /**
     * 追加数据到指定的预处理参数
     *
     * @param paramId
     * @param data
     * @throws IOException
     */
    default public void appendLongData(Integer paramId, byte[] data) throws IOException {
        Map<Integer, ByteArrayOutputStream> longDataMap = getLongDataMap();
        if (getLongData(paramId) == null) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(data);
            longDataMap.put(paramId, out);
        } else {
            longDataMap.get(paramId).write(data);
        }
    }

    default public void putLongDataForBuildLongData(int paramId, byte[] data) {
        Map<Integer, ByteArrayOutputStream> longDataMap = getLongDataMap();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            out.write(0x18);
            out.write(MySQLUtil.getFixIntByteArray(4, this.getStatementId()));
            out.write(MySQLUtil.getFixIntByteArray(2, paramId));
            out.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        longDataMap.put(paramId, out);
    }
}

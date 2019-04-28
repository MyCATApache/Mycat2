package io.mycat.proxy.task.prepareStatement;

import io.mycat.beans.mysql.MySQLPStmtBindValueList;
import io.mycat.proxy.packet.ColumnDefPacket;
import io.mycat.proxy.packet.MySQLPacket;
import io.mycat.proxy.session.MySQLSession;
import io.mycat.proxy.task.AsynTaskCallBack;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Map;

/**
 * CrazyPig,chenjunwen
 */
public interface PreparedStatement {
    long getStatementId();

    int getColumnsNumber();

    int getParametersNumber();

    Map<Integer, ByteArrayOutputStream> getLongDataMap();

    boolean setNewParameterBoundFlag(boolean b);

    default public OutputStream getLongData(int paramId) {
        return getLongDataMap().get(paramId);
    }

    int[] getParameterTypeList();

    ColumnDefPacket[] getColumnDefList();

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
            out.write(MySQLPacket.getFixIntByteArray(4, this.getStatementId()));
            out.write(MySQLPacket.getFixIntByteArray(2, paramId));
            out.write(data);
        } catch (IOException e) {
            e.printStackTrace();
        }
        longDataMap.put(paramId, out);
    }
}

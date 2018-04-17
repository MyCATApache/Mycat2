package io.mycat.mycat2.hbt;

import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.util.ParseUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 用于存储sql的结果集
 *
 * @author zhangwy
 */

public class TableMeta {
    public String table;
    public String alias;
    public List<List<byte[]>> fieldValues;
    public int fieldCount;
    public ResultSetMeta headerResultSetMeta;
    private byte packetId;
    private int writeRowDataIndex;

    public TableMeta(String table, String alias) {
        this.table = table;
        this.alias = alias;
    }

    public TableMeta() {
    }

    /**/
    public void init(int fieldCount) {
        this.fieldCount = fieldCount;
        headerResultSetMeta = new ResultSetMeta(fieldCount);
        this.fieldValues = new ArrayList<>();
    }

    public void init(ResultSetMeta resultSetMeta) {
        this.fieldCount = resultSetMeta.getFiledCount();
        if (this.fieldValues != null) throw new IllegalArgumentException("TableMeta has init");
        this.fieldValues = new ArrayList<>();
        headerResultSetMeta = resultSetMeta;

    }

    public void addFieldValues(List<byte[]> row) {
        fieldValues.add(row);
    }

    public ResultSetMeta getHeaderResultSet() {
        return headerResultSetMeta;
    }

    public void writeBegin(ProxyBuffer buffer) {
        this.packetId = 1;
        packetId = writeResultSetHeaderPacket(packetId, buffer);
        packetId = headerResultSetMeta.write(packetId, buffer);
        packetId = writeEofPacket(packetId, buffer);
        this.writeRowDataIndex = 0;
    }

    public void writeRowData(ProxyBuffer buffer) {

        for (int index = this.writeRowDataIndex; index < fieldValues.size(); index++) {

            RowDataPacket dataPacket = new RowDataPacket(fieldCount);
            List<byte[]> fieldValue = fieldValues.get(index);
            for (byte[] value : fieldValue) {
                dataPacket.add(value);
            }
            int size = dataPacket.calcPacketSize() + ParseUtil.msyql_packetHeaderSize;
            if (size <= buffer.getBuffer().remaining()) {
                dataPacket.packetId = packetId++;
                dataPacket.write(buffer);
                this.writeRowDataIndex++;
            } else {
                break;
            }
        }
        //是否可以写入Eof包
        if (5 <= buffer.getBuffer().remaining() && this.writeRowDataIndex == fieldValues.size()) {
            packetId = writeEofPacket(packetId, buffer);
            this.writeRowDataIndex++;
        }
    }

    public boolean isWriteFinish() {
        return this.writeRowDataIndex > fieldValues.size();
    }


    //	public void write(ProxyBuffer buffer) {
//		byte packetId = 1;
//		packetId = writeResultSetHeaderPacket(packetId, buffer);
//		packetId = headerResultSetMeta.write(packetId, buffer);
//		packetId = writeEofPacket(packetId, buffer);
//		packetId = writeRowData(packetId, buffer);
//		packetId = writeEofPacket(packetId, buffer);
//	}
//	
    private byte writeEofPacket(byte packetId, ProxyBuffer buffer) {
        EOFPacket eofPacket = new EOFPacket();
        eofPacket.packetId = packetId++;
        eofPacket.warningCount = 0;
        eofPacket.write(buffer);
        return packetId;
    }

    private byte writeResultSetHeaderPacket(byte packetId, ProxyBuffer buffer) {
        buffer.writeFixInt(3, ProxyBuffer.getLenencLength(fieldCount));
        buffer.writeByte(packetId++);
        buffer.writeLenencInt(fieldCount);
        return packetId;
    }

    //
    private byte writeRowData(byte packetId, ProxyBuffer buffer) {
        for (List<byte[]> fieldValue : fieldValues) {
//			int tmpWriteIndex = buffer.writeIndex;
//			buffer.writeIndex +=3;
//			buffer.writeByte(packetId ++);
//			for(byte[] value : fieldValue) {
//				buffer.writeLenencBytes(value);
//			}
//			//写入长度
//			buffer.putFixInt(tmpWriteIndex, 3, buffer.writeIndex - tmpWriteIndex - ParseUtil.msyql_packetHeaderSize);
//			
            RowDataPacket dataPacket = new RowDataPacket(fieldCount);
            for (byte[] value : fieldValue) {
                dataPacket.add(value);
            }
            dataPacket.packetId = packetId++;
            if (dataPacket.calcPacketSize() + ParseUtil.msyql_packetHeaderSize > buffer.getBuffer().remaining()) {
                dataPacket.write(buffer);
            }
        }
        return packetId;
    }

    public void print() {
        for (List<byte[]> fieldValue : fieldValues) {

            for (byte[] value : fieldValue) {
                System.out.print(String.format("%s  ", new String(value)));
            }
            System.out.println("");
        }
    }

    public String getFileds(String lJoinKey) {

        Integer pos = getHeaderResultSet().getFieldPos(lJoinKey);
        if (pos == null) throw new IllegalArgumentException("fileds not found joinkey " + lJoinKey);
        StringBuilder sb = new StringBuilder("");
        fieldValues.forEach(row -> {
            String value = new String(row.get(pos));
            sb.append("'").append(value).append("',");
        });
        String ids = "''";
        if (sb.length() > 1) {
            ids = sb.substring(0, sb.length() - 1);
        }
        return ids;

    }

    public Map<String, List<List<byte[]>>> getKeyMap(String key) {
        Map<String, List<List<byte[]>>> fieldMap = new HashMap<String, List<List<byte[]>>>();
        fieldValues.forEach(row -> {
            int pos = headerResultSetMeta.getFieldPos(key);
            String value = new String(row.get(pos));
            List<List<byte[]>> rowDataList = fieldMap.get(value);
            if (rowDataList == null) {
                rowDataList = new ArrayList<List<byte[]>>();
                fieldMap.put(value, rowDataList);
            }
            rowDataList.add(row);
        });
        return fieldMap;
    }

    public List<List<byte[]>> getFieldValues() {
        return fieldValues;
    }

    public void setFieldValues(List<List<byte[]>> fieldValues) {
        this.fieldValues = fieldValues;
    }

    @Override
    public String toString() {
        return "TableMeta{" +
                "table='" + table + '\'' +
                ", alias='" + alias + '\'' +
                ", fieldValues=" + fieldValues.stream().flatMap(i -> i.stream()).map(i -> new String(i))
                .collect(Collectors.joining(",")) +
                ", fieldCount=" + fieldCount +
                ", headerResultSetMeta=" + headerResultSetMeta +
                ", packetId=" + packetId +
                ", writeRowDataIndex=" + writeRowDataIndex +
                '}';
    }
}

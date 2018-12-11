package io.mycat.util.csv;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import cn.mrcode.mycat.fastcsv.CsvReader;
import cn.mrcode.mycat.fastcsv.DefaultCsvReader;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.FieldPacket;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * ${todo}
 *
 * @author : zhuqiang
 * @date : 2018/12/11 22:24
 */
public class CsvRead {
    private List<byte[]> fileds;
    private ColumnTypes[] types;
    private FieldPacket[] fieldPackets;
    private CsvReader reader;
    private EOFPacket eofPacket = new EOFPacket();
//    private Charset charset = Charset.forName("utf-8");

    public void init(Path path) throws IOException {
        reader = DefaultCsvReader.from(path);
        // 字段名称
        if (reader.hashNext()) {
            fileds = reader.next();
        } else {
            throw new RuntimeException("initialization failed: The column name row must exist");
        }
        // 字段类型
        if (reader.hashNext()) {
            List<byte[]> ts = reader.next();
            if (ts.size() != fileds.size()) {
                throw new RuntimeException("The column name does not match the column type length");
            }
            types = new ColumnTypes[ts.size()];
            for (int i = 0; i < ts.size(); i++) {
                String type = new String(ts.get(i));
                ColumnTypes columnType = ColumnTypes.find(type);
                if (columnType == null) {
                    throw new IllegalArgumentException("column type " + type + " nonsupport");
                }
                types[i] = columnType;
            }
        } else {
            throw new RuntimeException("initialization failed: The column type row must exist");
        }

        fieldPackets = builderFieldPackets();
    }

    public void read(int num, byte initPacketId, ProxyBuffer proxyBuffer) throws IOException {
        byte currentPacketId = initPacketId;
        writeHeaderPacket(proxyBuffer, ++currentPacketId);
        currentPacketId = writeFieldPacket(proxyBuffer, currentPacketId);
        RowDataPacket rowDataPacket = new RowDataPacket(fileds.size());
        for (int i = 0; i < num; i++) {
            if (reader.hashNext()) {
                List<byte[]> next = reader.next();
                rowDataPacket.fieldCount = next.size();
                rowDataPacket.packetId = ++currentPacketId;
                rowDataPacket.fieldValues.clear();
                for (byte[] bytes : next) {
                    rowDataPacket.add(bytes);
                }
                rowDataPacket.write(proxyBuffer);
            } else {
                break;
            }
        }
        eofPacket.packetId = ++currentPacketId;
        eofPacket.write(proxyBuffer);
        proxyBuffer.flip();
        proxyBuffer.readIndex = proxyBuffer.writeIndex;
    }


    private FieldPacket[] builderFieldPackets() {
        int size = fileds.size();
        FieldPacket[] fieldPackets = new FieldPacket[size];
        for (int i = 0; i < size; i++) {
            fieldPackets[i] = builderFieldPacket(fileds.get(i), types[i]);
        }
        return fieldPackets;
    }

    private FieldPacket builderFieldPacket(byte[] filed, ColumnTypes type) {
        FieldPacket fieldPacket = new FieldPacket();
        fieldPacket.db = "db".getBytes();
        fieldPacket.table = "table".getBytes();
        fieldPacket.orgTable = fieldPacket.table;
        fieldPacket.name = filed;
        fieldPacket.orgName = filed;
        // TODO: 2018/12/11 utf8_general_ci
        fieldPacket.charsetIndex = 33;
        fieldPacket.length = type.getM();
        fieldPacket.type = type.getValue();
        fieldPacket.flags = type.getStatus();
        fieldPacket.decimals = type.getD();
        return fieldPacket;
    }

    private void writeHeaderPacket(ProxyBuffer proxyBuffer, byte packetId) {
        ResultSetHeaderPacket headerPacket = new ResultSetHeaderPacket();
        headerPacket.fieldCount = fieldPackets.length;
        headerPacket.packetId = packetId;
        headerPacket.write(proxyBuffer);
    }

    private byte writeFieldPacket(ProxyBuffer proxyBuffer, byte initPacketId) {
        byte currentPacketId = initPacketId;
        for (FieldPacket fieldPacket : fieldPackets) {
            fieldPacket.packetId = ++currentPacketId;
            fieldPacket.write(proxyBuffer);
        }
        eofPacket.packetId = ++currentPacketId;
        eofPacket.write(proxyBuffer);
        return currentPacketId;
    }
}

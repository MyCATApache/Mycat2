package io.mycat.util.csv;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.FieldPacket;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * 伪造数据测试用例
 *
 * @author : zhuqiang
 * @date : 2019/3/16 16:42
 */
public class ColumnTypesTest {
    public static void main(String[] args) {
        mock(new ProxyBuffer(ByteBuffer.allocate(1024)));
    }

    public static ProxyBuffer mock(ProxyBuffer buffer) {
        ResultSetHeaderPacket headerPacket = new ResultSetHeaderPacket();
        headerPacket.fieldCount = 5;
        headerPacket.packetId = 1;

        FieldPacket id = buildBigint("id", 20);
        id.packetId = 2;
        // 如果是 utf 8 ，varchar 的长度需要 * 3
        FieldPacket userId = buildVarchar("user_id", 5 * 3);
        userId.packetId = 3;
        FieldPacket date = buildDate("date");
        date.packetId = 4;
        FieldPacket fee = buildDecimal("fee", 10, (byte) 0);
        fee.packetId = 5;
        FieldPacket days = buildInt("days", 11);
        days.packetId = 6;

        RowDataPacket r1 = buildRdp("0", "2", "2018-11-02", "2", "2");
        r1.packetId = 8;
        RowDataPacket r2 = buildRdp("1", "2", "2018-11-02", "2", "2");
        r2.packetId = 9;
        RowDataPacket r3 = buildRdp("9", "开源中", "", "10", "0");
        r3.packetId = 10;

//        ProxyBuffer buffer = new ProxyBuffer(ByteBuffer.allocate(1024));
        headerPacket.write(buffer);
        id.write(buffer);
        userId.write(buffer);
        date.write(buffer);
        fee.write(buffer);
        days.write(buffer);
        buildEof((byte) 7).write(buffer);
        r1.write(buffer);
        r2.write(buffer);
        r3.write(buffer);
        buildEof((byte) 11).write(buffer);

        buffer.flip();
        buffer.readIndex = buffer.writeIndex;
        return buffer;
    }

    public static FieldPacket buildBigint(String name, int length) {
        FieldPacket fieldPacket = new FieldPacket();
        fieldPacket.db = "db".getBytes();
        fieldPacket.table = "table".getBytes();
        fieldPacket.orgTable = fieldPacket.table;
        fieldPacket.name = name.getBytes();
        fieldPacket.orgName = fieldPacket.name;

        ColumnTypes type = ColumnTypes.MYSQL_TYPE_LONGLONG;
        fieldPacket.charsetIndex = type.getCcharetIndex();
        fieldPacket.length = length;
        fieldPacket.type = type.getValue();
        fieldPacket.flags = type.getStatus();
        fieldPacket.decimals = type.getD();
        return fieldPacket;
    }

    public static FieldPacket buildVarchar(String name, int length) {
        FieldPacket fieldPacket = new FieldPacket();
        fieldPacket.db = "db".getBytes();
        fieldPacket.table = "table".getBytes();
        fieldPacket.orgTable = fieldPacket.table;
        fieldPacket.name = name.getBytes();
        fieldPacket.orgName = fieldPacket.name;

        ColumnTypes type = ColumnTypes.VARBINARY_MYSQL_TYPE_VAR_STRING;
        fieldPacket.charsetIndex = type.getCcharetIndex();
        fieldPacket.length = length;
        fieldPacket.type = type.getValue();
        fieldPacket.flags = type.getStatus();
        fieldPacket.decimals = type.getD();
        return fieldPacket;
    }

    public static FieldPacket buildDate(String name) {
        FieldPacket fieldPacket = new FieldPacket();
        fieldPacket.db = "db".getBytes();
        fieldPacket.table = "table".getBytes();
        fieldPacket.orgTable = fieldPacket.table;
        fieldPacket.name = name.getBytes();
        fieldPacket.orgName = fieldPacket.name;

        ColumnTypes type = ColumnTypes.MYSQL_TYPE_DATE;
        fieldPacket.charsetIndex = type.getCcharetIndex();
        fieldPacket.length = type.getM();
        fieldPacket.type = type.getValue();
        fieldPacket.flags = type.getStatus();
        fieldPacket.decimals = type.getD();
        return fieldPacket;
    }

    /**
     * @param m 宽度
     * @param d 小数点位数
     */
    public static FieldPacket buildDecimal(String name, int m, byte d) {
        FieldPacket fieldPacket = new FieldPacket();
        fieldPacket.db = "db".getBytes();
        fieldPacket.table = "table".getBytes();
        fieldPacket.orgTable = fieldPacket.table;
        fieldPacket.name = name.getBytes();
        fieldPacket.orgName = fieldPacket.name;

        ColumnTypes type = ColumnTypes.MYSQL_TYPE_DATE;
        fieldPacket.charsetIndex = type.getCcharetIndex();
        fieldPacket.length = type.getM();
        fieldPacket.type = type.getValue();
        fieldPacket.flags = type.getStatus();
        fieldPacket.decimals = type.getD();
        return fieldPacket;
    }

    public static FieldPacket buildInt(String name, int m) {
        FieldPacket fieldPacket = new FieldPacket();
        fieldPacket.db = "db".getBytes();
        fieldPacket.table = "table".getBytes();
        fieldPacket.orgTable = fieldPacket.table;
        fieldPacket.name = name.getBytes();
        fieldPacket.orgName = fieldPacket.name;

        ColumnTypes type = ColumnTypes.MYSQL_TYPE_LONG;
        fieldPacket.charsetIndex = type.getCcharetIndex();
        fieldPacket.length = m;
        fieldPacket.type = type.getValue();
        fieldPacket.flags = type.getStatus();
        fieldPacket.decimals = type.getD();
        fieldPacket.charsetIndex = 63;
        return fieldPacket;
    }

    public static RowDataPacket buildRdp(String... v1) {
        RowDataPacket rowDataPacket = new RowDataPacket(5);
        for (String s : v1) {
            rowDataPacket.add(s.getBytes(Charset.forName("utf-8")));
        }
        return rowDataPacket;
    }

    public static EOFPacket buildEof(byte packetId) {
        EOFPacket eofPacket = new EOFPacket();
        eofPacket.packetId = packetId;
        return eofPacket;
    }
}

// 从 csv 中读取数据，伪造数据表代码示例

/*public class CsvRead {
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
}*/

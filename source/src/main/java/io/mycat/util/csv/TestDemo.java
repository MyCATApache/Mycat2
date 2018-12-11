package io.mycat.util.csv;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.FieldPacket;
import io.mycat.mysql.packet.ResultSetHeaderPacket;
import io.mycat.mysql.packet.RowDataPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * ${todo}
 *
 * @author : zhuqiang
 * @date : 2018/12/1 23:04
 */
public class TestDemo {
//    public static void main(String[] args) throws IOException {
//        CsvReader reader = DefaultCsvReader.from(Paths.get("C:\\Users\\mrcode\\Desktop\\travelrecord.csv"));
//        reader.hashNext();
//        // 字段名
//        List<byte[]> fields = reader.next();
//        // 数据类型
//        reader.hashNext();
//        List<byte[]> dataTypes = reader.next();
//
//        // ResultSetHeaderPacket
//        ResultSetHeaderPacket resultSetHeaderPacket = new ResultSetHeaderPacket();
//        resultSetHeaderPacket.fieldCount = fields.size();
//        // FieldPacket
//        for (int i = 0; i < fields.size(); i++) {
//            FieldPacket fieldPacket = new FieldPacket();
//            fieldPacket.db = "db".getBytes();
//            fieldPacket.table = "table".getBytes();
//            fieldPacket.orgTable = fieldPacket.table;
//            fieldPacket.name = fields.get(i);
//            fieldPacket.orgName = fieldPacket.name;
//            fieldPacket.charsetIndex = 33;
////            fieldPacket.charsetIndex
//        }
//        // RowDataPacket
//    }

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
        RowDataPacket r3 = buildRdp("9", "朱强强强2", "", "10", "0");
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
        fieldPacket.charsetIndex = 63;
        fieldPacket.length = length;
        fieldPacket.type = ColumnTypes.MYSQL_TYPE_LONGLONG.getValue();
        fieldPacket.flags = 0;
        fieldPacket.decimals = 0;
        return fieldPacket;
    }

    public static FieldPacket buildVarchar(String name, int length) {
        FieldPacket fieldPacket = new FieldPacket();
        fieldPacket.db = "db".getBytes();
        fieldPacket.table = "table".getBytes();
        fieldPacket.orgTable = fieldPacket.table;
        fieldPacket.name = name.getBytes();
        fieldPacket.orgName = fieldPacket.name;
        fieldPacket.charsetIndex = 33;
        fieldPacket.length = length;
        fieldPacket.type = ColumnTypes.VARBINARY_MYSQL_TYPE_VAR_STRING.getValue();
        fieldPacket.flags = 0;
        fieldPacket.decimals = 0;
        return fieldPacket;
    }

    public static FieldPacket buildDate(String name) {
        FieldPacket fieldPacket = new FieldPacket();
        fieldPacket.db = "db".getBytes();
        fieldPacket.table = "table".getBytes();
        fieldPacket.orgTable = fieldPacket.table;
        fieldPacket.name = name.getBytes();
        fieldPacket.orgName = fieldPacket.name;
        fieldPacket.charsetIndex = 63;
        fieldPacket.length = 10;
        fieldPacket.type = ColumnTypes.MYSQL_TYPE_DATE.getValue();
        fieldPacket.flags = 0;
        fieldPacket.decimals = 0;
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
        fieldPacket.charsetIndex = 63;
        fieldPacket.length = m + 1; // 小数点
        fieldPacket.type = ColumnTypes.MYSQL_TYPE_DATE.getValue();
        fieldPacket.flags = 0;
        fieldPacket.decimals = d;
        return fieldPacket;
    }

    public static FieldPacket buildInt(String name, int m) {
        FieldPacket fieldPacket = new FieldPacket();
        fieldPacket.db = "db".getBytes();
        fieldPacket.table = "table".getBytes();
        fieldPacket.orgTable = fieldPacket.table;
        fieldPacket.name = name.getBytes();
        fieldPacket.orgName = fieldPacket.name;
        fieldPacket.charsetIndex = 63;
        fieldPacket.length = m;
        fieldPacket.type = ColumnTypes.MYSQL_TYPE_LONG.getValue();
        fieldPacket.flags = 0;
        fieldPacket.decimals = 0;
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

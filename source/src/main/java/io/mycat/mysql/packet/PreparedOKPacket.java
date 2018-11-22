package io.mycat.mysql.packet;

import io.mycat.proxy.ProxyBuffer;

/**
 * <pre>
 * From server to client, in response to prepared statement initialization packet.
 * It is made up of:
 *   1.a PREPARE_OK packet
 *   2.if "number of parameters" > 0
 *       (field packets) as in a Result Set Header Packet
 *       (EOF packet)
 *   3.if "number of columns" > 0
 *       (field packets) as in a Result Set Header Packet
 *       (EOF packet)
 *
 * -----------------------------------------------------------------------------------------
 *
 *  Bytes              Name
 *  -----              ----
 *  1                  0 - marker for OK packet
 *  4                  statement_handler_id
 *  2                  number of columns in result set
 *  2                  number of parameters in query
 *  1                  filler (always 0)
 *  2                  warning count
 *
 *  @see https://dev.mysql.com/doc/internals/en/com-stmt-prepare-response.html
 * </pre>
 *
 * @author linxiaofang
 * @date 2018/11/12
 */
public class PreparedOKPacket extends MySQLPacket {
    public byte status;
    public long statementId;
    public int columnsNumber;
    public int parametersNumber;
    public byte filler;
    public int warningCount;
    public ColumnDefinitionPacket[] parameterDefinitions;
    public EOFPacket parameterEOFPacket;
    public ColumnDefinitionPacket[] columnDefinitions;
    public EOFPacket columnEOFPacket;

    public void read(ProxyBuffer buffer) {
        // packet length:3
        packetLength = (int)buffer.readFixInt(3);
        // packet number:1
        packetId = buffer.readByte();
        readPayload(buffer);
    }

    public void readPayload(ProxyBuffer buffer) {
        // payload
        status = buffer.readByte();
        statementId = buffer.readFixInt(4);
        columnsNumber = (int)buffer.readFixInt(2);
        parametersNumber = (int)buffer.readFixInt(2);
        filler = buffer.readByte();
        warningCount = (int)buffer.readFixInt(2);
        if (parametersNumber > 0) {
            parameterDefinitions = new ColumnDefinitionPacket[parametersNumber];
            for (int i=0; i<parametersNumber; i++) {
                ColumnDefinitionPacket columnDefinition = new ColumnDefinitionPacket(MySQLPacket.COM_STMT_PREPARE);
                columnDefinition.read(buffer);
                parameterDefinitions[i] = columnDefinition;
            }
            parameterEOFPacket = new EOFPacket();
            parameterEOFPacket.read(buffer);
        }
        if (columnsNumber > 0) {
            columnDefinitions = new ColumnDefinitionPacket[columnsNumber];
            for (int i=0; i<columnsNumber; i++) {
                ColumnDefinitionPacket columnDefinition = new ColumnDefinitionPacket(MySQLPacket.COM_STMT_PREPARE);
                columnDefinition.read(buffer);
                columnDefinitions[i] = columnDefinition;
            }
            columnEOFPacket = new EOFPacket();
            columnEOFPacket.read(buffer);
        }
    }

    @Override
    public void write(ProxyBuffer buffer) {
        // write packet length
        buffer.writeFixInt(3, packetLength);
        // write packet number
        buffer.writeByte(packetId);
        // write payload
        buffer.writeByte(status);
        buffer.writeFixInt(4, statementId);
        buffer.writeFixInt(2, columnsNumber);
        buffer.writeFixInt(2, parametersNumber);
        buffer.writeByte(filler);
        buffer.writeFixInt(2, warningCount);
        for (int i=0; i<parametersNumber; i++) {
            parameterDefinitions[i].write(buffer);
        }
        parameterEOFPacket.write(buffer);
        for (int i=0; i<columnsNumber; i++) {
            columnDefinitions[i].write(buffer);
        }
        columnEOFPacket.write(buffer);
    }

    @Override
    public int calcPacketSize() {
        return 12;
    }

    @Override
    protected String getPacketInfo() {
        return "Prepared OK Packet";
    }
}

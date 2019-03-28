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
    public ColumnDefPacket[] parameterDefinitions;
    public ColumnDefPacket[] columnDefinitions;

    @Override
    public void writePayload(ProxyBuffer buffer) {
        // write payload
        buffer.writeByte(status);
        buffer.writeFixInt(4, statementId);
        buffer.writeFixInt(2, columnsNumber);
        buffer.writeFixInt(2, parametersNumber);
        buffer.writeByte(filler);
        buffer.writeFixInt(2, warningCount);
    }
    @Override
    public void readPayload(ProxyBuffer buffer) {
        // payload
        status = buffer.readByte();
        statementId = buffer.readFixInt(4);
        columnsNumber = (int)buffer.readFixInt(2);
        parametersNumber = (int)buffer.readFixInt(2);
        filler = buffer.readByte();
        warningCount = (int)buffer.readFixInt(2);
    }

    @Override
    public int calcPayloadSize() {
        return 12;
    }

    @Override
    protected String getPacketInfo() {
        return "Prepared DEFAULT_OK_PACKET Packet";
    }
}

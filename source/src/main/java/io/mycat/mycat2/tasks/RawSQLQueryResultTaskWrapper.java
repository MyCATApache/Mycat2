package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.MySQLPackageInf;
import io.mycat.mycat2.net.MainMySQLNIOHandler;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public abstract class RawSQLQueryResultTaskWrapper extends BackendIOTaskWithResultSet<MySQLSession> {
    private static Logger logger = LoggerFactory.getLogger(RawSQLQueryResultTaskWrapper.class);

    public void fetchSQL(QueryPacket queryPacket) throws IOException {
        /*设置为忙*/
        session.setIdle(false);
        ProxyBuffer proxyBuf = session.proxyBuffer;
        proxyBuf.reset();
        queryPacket.write(proxyBuf);
        session.setCurNIOHandler(this);
        proxyBuf.flip();
        proxyBuf.readIndex = proxyBuf.writeIndex;
        this.session.writeToChannel();
    }

    @Override
    void onRsColCount(MySQLSession session) {
        ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMSQLPackgInf = session.curMSQLPackgInf;
        onRsColCount(session, (int) proxyBuffer.getLenencInt(curMSQLPackgInf.startPos + MySQLPacket.packetHeaderSize));
    }

    /**
     * * <pre>
     * Bytes                      Name
     * -----                      ----
     * n (Length Coded String)    catalog
     * n (Length Coded String)    db
     * n (Length Coded String)    table
     * n (Length Coded String)    org_table
     * n (Length Coded String)    name
     * n (Length Coded String)    org_name
     * 1                          (filler)
     * 2                          charsetNumber
     * 4                          length
     * 1                          type
     * 2                          flags
     * 1                          decimals
     * 2                          (filler), always 0x00
     * n (Length Coded Binary)    default
     * <p>
     *  http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Field_Packet
     * </pre>
     *
     * @param session
     */
    @Override
    void onRsColDef(MySQLSession session) {
        ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;

        int tmpReadIndex = proxyBuffer.readIndex;
        int rowDataIndex = curMQLPackgInf.startPos + MySQLPacket.packetHeaderSize;
        proxyBuffer.readIndex = rowDataIndex;

        String catalog = proxyBuffer.readLenencString().intern();  //catalog
        String schema = proxyBuffer.readLenencString().intern();  //mycatSchema
        String table = proxyBuffer.readLenencString().intern();  //table
        String orgTable = proxyBuffer.readLenencString().intern();  //orgTable
        String name = proxyBuffer.readLenencString().intern();  //name
        String orgName = proxyBuffer.readLenencString().intern();

        //proxyBuffer.readBytes(7); // 1(filler) + 2(charsetNumber) + 4 (length)
        byte filler = proxyBuffer.readByte();
        int charsetNumber = proxyBuffer.readByte() << 1 & proxyBuffer.readByte();
        int length = (int) proxyBuffer.readFixInt(4);

        int fieldType = proxyBuffer.readByte() & 0xff;
        onRsColDef(session,
                catalog,
                schema,
                table,
                orgTable,
                name,
                orgName,
                filler,
                charsetNumber,
                length,
                fieldType
        );

        proxyBuffer.readIndex = tmpReadIndex;
    }

    @Override
    void onRsRow(MySQLSession session) {
        ProxyBuffer proxyBuffer = session.proxyBuffer;
        MySQLPackageInf curMQLPackgInf = session.curMSQLPackgInf;
        int rowDataIndex = curMQLPackgInf.startPos + MySQLPacket.packetHeaderSize;
        int tmpReadIndex = proxyBuffer.readIndex;
        proxyBuffer.readIndex = rowDataIndex;

        onRsRow(session, proxyBuffer);

        proxyBuffer.readIndex = tmpReadIndex;

    }

    abstract void onRsColCount(MySQLSession session, int fieldCount);

    /**
     * the param's type is Object beacuse it may be byte[],String,ByteBuffer even null
     *
     * @param session
     * @param catalog
     * @param schema
     * @param table
     * @param orgTable
     * @param name
     * @param org_name
     * @param filler
     * @param charsetNumber
     * @param length
     * @param fieldType
     */
    abstract void onRsColDef(MySQLSession session,
                             Object catalog,
                             Object schema,
                             Object table,
                             Object orgTable,
                             Object name,
                             Object org_name,
                             byte filler,
                             int charsetNumber,
                             int length,
                             int fieldType);

    /**
     * read the row field by readLenencString or readLenencBytes
     *
     * @param session
     * @param proxyBuffer
     */
    abstract void onRsRow(MySQLSession session, ProxyBuffer proxyBuffer);

    abstract void onError(MySQLSession session, String msg);

    abstract void onRsFinished(MySQLSession session);

    public void clearResouces() {
        session.setIdle(true);
        revertPreBuffer();
        session.setCurNIOHandler(MainMySQLNIOHandler.INSTANCE);
    }

    @Override
    void onRsFinish(MySQLSession session, boolean success, String msg) throws IOException {
        if (!success) {
            onError(session, msg);
            if (callBack != null) {
                callBack.finished(session, this, success, msg);
            }
        } else {
            onRsFinished(session);
            if (callBack != null)
                callBack.finished(session, null, success, msg);
        }
    }

    /**
     * @param userSession
     * @param normal
     * @todo check need Override this function
     */
    @Override
    public void onSocketClosed(MySQLSession userSession, boolean normal) {
        clearResouces();
    }
}

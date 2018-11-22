package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.CurrPacketType;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

import java.io.IOException;

/**
 * 
 * <b><code>MultiNodeBackendTaskWithGenericResponse</code></b>
 * <p>
 * Generic Response Packet（如OK_Packet,ERR_Packet）的Task处理类.
 * </p>
 * <b>Creation Time:</b> 2018-04-28
 * 
 * @author <a href="mailto:flysqrlboy@gmail.com">zhangsiwei</a>
 * @since 2.0
 */
public abstract class BackendIOTaskWithGenericResponse
        extends AbstractBackendIOTask<MySQLSession> {

    public void excecuteSQL(String sql) throws IOException {
        QueryPacket queryPacket = new QueryPacket();
        queryPacket.packetId = 0;
        queryPacket.sql = sql;
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
    public void onSocketRead(MySQLSession session) throws IOException {

        if (!session.readFromChannel()) {
            return;
        }
        CurrPacketType currPacketType = session.resolveMySQLPackage(true);

        session.proxyBuffer.flip();
        if (currPacketType == CurrPacketType.Full) {

            try {
                if (session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
                    onOkResponse(session);
                } else if (session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET) {
                    onErrResponse(session);
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                onFinished(false, session);
            }
            onFinished(true, session);

        } else {
            return;
        }
    }

    public abstract void onOkResponse(MySQLSession session) throws IOException;

    public abstract void onErrResponse(MySQLSession session) throws IOException;

    public abstract void onFinished(boolean success, MySQLSession session) throws IOException;

}

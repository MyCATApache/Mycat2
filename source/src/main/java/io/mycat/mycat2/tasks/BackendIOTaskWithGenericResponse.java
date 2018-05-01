package io.mycat.mycat2.tasks;

import java.io.IOException;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.console.SessionKeyEnum;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

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
        session.getSessionAttrMap().put(SessionKeyEnum.SESSION_KEY_CONN_IDLE_FLAG.getKey(), false);
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
        AbstractMySQLSession.CurrPacketType currPacketType =
                session.resolveMySQLPackage(session.proxyBuffer, session.curMSQLPackgInf, false);

        session.proxyBuffer.flip();
        if (currPacketType == AbstractMySQLSession.CurrPacketType.Full) {

            try {
                if (session.curMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
                    onOkResponse(session);
                } else if (session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET) {
                    onErrResponse(session);
                }
            } catch (Exception ex) {
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

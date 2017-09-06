package io.mycat.mycat2.tasks;

import java.io.IOException;

import io.mycat.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.CommandPacket;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

/**
 * Created by ynfeng on 2017/8/13.
 * <p>
 * 同步状态至后端数据库，包括：字符集，事务，隔离级别等
 */
public class BackendSynchronzationTask extends AbstractBackendIOTask<MySQLSession> {
    private static Logger logger = LoggerFactory.getLogger(BackendSynchronzationTask.class);
    
    private int syncCmdNum = 0;

    public BackendSynchronzationTask(MySQLSession session) throws IOException {
        super(session,true);
        syncState(session);
    }

    private void syncState(MySQLSession session) throws IOException {
        logger.info("synchronzation state to bakcend.session=" + session.toString());
        ProxyBuffer proxyBuf = session.proxyBuffer;
        proxyBuf.reset();
        String charset = null;
        System.out.println("charset.........................."+session.charSet);
        if(session.charSet!=null) {
            charset = CharsetUtil.getCharset(Integer.parseInt(session.charSet.charsetIndex)) + " ;";
        }
        QueryPacket queryPacket = new QueryPacket();
        queryPacket.packetId = 0;
        queryPacket.sql = session.isolation.getCmd() + session.autoCommit.getCmd() + session.isolation.getCmd()+
                        session.charSet.getCmd()+charset;
        if(session.charSet!=null && charset!=null){
            queryPacket.sql +=queryPacket.sql+session.charSet.getCmd()+charset;
        }
        syncCmdNum = 3;
        queryPacket.write(proxyBuf);
        proxyBuf.flip();
        proxyBuf.readIndex = proxyBuf.writeIndex;
        session.writeToChannel();
    }

    @Override
    public void onSocketRead(MySQLSession session) throws IOException {
        session.proxyBuffer.reset();
        if (!session.readFromChannel()) {// 没有读到数据或者报文不完整
            return;
        }
        boolean isAllOK = true;
        while (syncCmdNum >0) {
        	switch (session.resolveMySQLPackage(session.proxyBuffer, session.curMSQLPackgInf, true)) {
			case Full:
				if(session.curMSQLPackgInf.pkgType == MySQLPacket.ERROR_PACKET){
					isAllOK = false;
					syncCmdNum = 0;
				}
				break;
			default:
				return;
        	}
        	syncCmdNum --;
        }

        if (isAllOK) {
            this.finished(true);
        } else {
            errPkg = new ErrorPacket();
            errPkg.read(session.proxyBuffer);
            logger.warn("backend state sync Error.Err No. " + errPkg.errno + "," + errPkg.message);
            this.finished(false);
        }
    }
}

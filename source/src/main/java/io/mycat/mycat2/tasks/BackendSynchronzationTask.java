package io.mycat.mycat2.tasks;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

import javax.management.Query;

/**
 * Created by ynfeng on 2017/8/13.
 * <p>
 * 同步状态至后端数据库，包括：字符集，事务，隔离级别等
 */
public class BackendSynchronzationTask extends AbstractBackendIOTask {
    private static Logger logger = LoggerFactory.getLogger(BackendSynchronzationTask.class);

    public BackendSynchronzationTask(MySQLSession session) throws IOException {
        super(session);
        syncState(session);
    }

    private void syncState(MySQLSession session) throws IOException {
        logger.info("synchronzation state to bakcend.session=" + session.toString());
        ProxyBuffer frontBuffer = session.frontBuffer;
        frontBuffer.reset();
        // TODO 字符集映射未完成
        QueryPacket queryPacket = new QueryPacket();
        queryPacket.packetId = 0;
        queryPacket.sql = session.isolation.getCmd() + session.autoCommit.getCmd() + session.isolation.getCmd();
        queryPacket.write(frontBuffer);
        frontBuffer.flip();
        session.writeToChannel(frontBuffer, session.backendChannel);
    }

    @Override
    public void onBackendRead(MySQLSession session) throws IOException {
        session.frontBuffer.reset();
        if (!session.readFromChannel(session.frontBuffer, session.backendChannel)
                || !session.resolveMySQLPackage(session.frontBuffer, session.curBackendMSQLPackgInf, false)) {// 没有读到数据或者报文不完整
            return;
        }
        if (session.curBackendMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
            session.frontBuffer.reset();
            this.finished(true);
        } else {
            errPkg = new ErrorPacket();
            errPkg.read(session.frontBuffer);
            logger.warn("backend state sync Error.Err No. " + errPkg.errno + "," + errPkg.message);
            this.finished(false);
        }
    }

    @Override
    public void onBackendSocketClosed(MySQLSession userSession, boolean normal) {
        logger.warn(" socket closed not handlerd" + session.toString());
    }

}

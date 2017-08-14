package io.mycat.mycat2.tasks;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Created by ynfeng on 2017/8/13.
 * <p>
 * 同步状态至后端数据库，包括：字符集，事务，隔离级别等
 */
public class BackendSynchronzationTask implements BackendIOTask<MySQLSession> {
    private static Logger logger = LoggerFactory.getLogger(BackendSynchronzationTask.class);
    private AsynTaskCallBack callBack;
    private MySQLSession session;
    private static QueryPacket[] CMDS = new QueryPacket[3];
    private int processCmd = 0;
    private ErrorPacket errPkg;

    static {
        QueryPacket isolationSynCmd = new QueryPacket();
        isolationSynCmd.packetId = 0;

        QueryPacket charsetSynCmd = new QueryPacket();
        charsetSynCmd.packetId = 0;

        QueryPacket transactionSynCmd = new QueryPacket();
        transactionSynCmd.packetId = 0;

        CMDS[0] = isolationSynCmd;
        CMDS[1] = charsetSynCmd;
        CMDS[2] = transactionSynCmd;
    }

    public BackendSynchronzationTask(MySQLSession session) throws IOException {
        this.processCmd = 0;
        this.session = session;
        syncState(session);
    }

    private void syncState(MySQLSession session) throws IOException {
        logger.info("synchronzation state to bakcend.session=" + session.toString());
        ProxyBuffer frontBuffer = session.frontBuffer;
        frontBuffer.reset();
        //TODO 字符集映射和前端事务设置还未完成，这里只用隔离级别模拟实现(其实都是SET xxx效果一样)，回头补充
        switch (processCmd) {
            case 1:
            case 2:
            case 0:
                CMDS[processCmd].sql = session.isolation.getCmd();
                CMDS[processCmd].write(frontBuffer);

                frontBuffer.flip();
                session.writeToChannel(frontBuffer, session.backendChannel);
                processCmd++;
                break;
            default:
                this.finished(true);
                break;
        }

    }

    @Override
    public void onBackendConnect(MySQLSession userSession, boolean success, String msg) throws IOException {

    }

    @Override
    public void onBackendRead(MySQLSession session) throws IOException {
        session.frontBuffer.reset();
        if (!session.readSocket(false)
                || !session.resolveMySQLPackage(session.frontBuffer, session.curBackendMSQLPackgInf, false)) {// 没有读到数据或者报文不完整
            return;
        }
        if (session.curBackendMSQLPackgInf.pkgType == MySQLPacket.OK_PACKET) {
            syncState(session);
        } else {
            //TODO 同步失败如何处理？？是否应该关闭此连接？？
            errPkg = new ErrorPacket();
            errPkg.read(session.frontBuffer);
            logger.warn("backend state sync Error.Err No. " + errPkg.errno + "," + errPkg.message);
            this.finished(false);
        }
    }

    private void finished(boolean success) throws IOException {
        callBack.finished(session, this, success, this.errPkg);
    }

    @Override
    public void onBackendWrite(MySQLSession session) throws IOException {

    }

    @Override
    public void onBackendSocketClosed(MySQLSession userSession, boolean normal) {

    }

    @Override
    public void setCallback(AsynTaskCallBack callBack) {
        this.callBack = callBack;
    }
}
